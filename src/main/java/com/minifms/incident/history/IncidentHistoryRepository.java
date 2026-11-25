package com.minifms.incident.history;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.minifms.common.config.properties.IncidentHistoryProperties;
import com.minifms.incident.IncidentProcessingResult;
import com.minifms.incident.IncidentType;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.stereotype.Repository;

/**
 * 돌발상황 관제 이력 저장소(Elasticsearch).
 *
 * <p>쓰기는 external 버전으로 한다(문서 ID = incidentId, 버전 = 처리 결과 version). Elasticsearch 는 저장된 것보다
 * 큰 버전만 받아들이므로(409 version_conflict), 같은 결과의 재전달은 문서를 늘리지 않고, 늦게 도착한 이전 결과는
 * 최신 결과를 덮어쓰지 않는다. 소비자 쪽 순서 보장이나 별도 잠금이 필요 없다.
 */
@Repository
public class IncidentHistoryRepository {

    private final ElasticsearchClient client;
    private final IncidentHistoryProperties properties;
    private final AtomicBoolean indexReady = new AtomicBoolean();

    public IncidentHistoryRepository(ElasticsearchClient client, IncidentHistoryProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    /** 색인이 없으면 매핑을 지정해 만든다. 앱 기동 때가 아니라 첫 색인 때 하므로 Elasticsearch 가 꺼진 채로도 앱은 뜬다. */
    public void ensureIndex() throws IOException {
        if (indexReady.get()) {
            return;
        }
        if (!client.indices().exists(e -> e.index(properties.index())).value()) {
            try {
                client.indices().create(c -> c
                        .index(properties.index())
                        .mappings(m -> m
                                .dynamic(DynamicMapping.Strict)
                                .properties("incidentId", p -> p.keyword(k -> k))
                                .properties("version", p -> p.long_(l -> l))
                                .properties("type", p -> p.keyword(k -> k))
                                .properties("sourceId", p -> p.keyword(k -> k))
                                .properties("location", p -> p.geoPoint(g -> g))
                                .properties("occurredAt", p -> p.date(d -> d))
                                .properties("description", p -> p.text(t -> t))
                                .properties("status", p -> p.keyword(k -> k))
                                .properties("targetCount", p -> p.integer(i -> i))
                                .properties("confirmedCount", p -> p.integer(i -> i))
                                .properties("failedCount", p -> p.integer(i -> i))
                                .properties("processedAt", p -> p.date(d -> d))));
            } catch (ElasticsearchException e) {
                if (!"resource_already_exists_exception".equals(errorType(e))) {
                    throw e; // 다른 인스턴스가 먼저 만든 경우만 괜찮다
                }
            }
        }
        indexReady.set(true);
    }

    /** 처리 결과를 한 번의 bulk 요청으로 색인한다. */
    public BulkOutcome indexAll(Collection<IncidentProcessingResult> results) throws IOException {
        ensureIndex();
        BulkRequest.Builder bulk = new BulkRequest.Builder();
        for (IncidentProcessingResult result : results) {
            bulk.operations(op -> op.index(index -> index
                    .index(properties.index())
                    .id(result.incidentId())
                    .version(result.version())
                    .versionType(VersionType.External)
                    .document(IncidentHistoryDocument.from(result))));
        }
        BulkResponse response = client.bulk(bulk.build());

        int indexed = 0;
        int stale = 0;
        List<String> retryable = new ArrayList<>();
        List<String> rejected = new ArrayList<>();
        for (BulkResponseItem item : response.items()) {
            if (item.error() == null) {
                indexed++;
            } else if (item.status() == 409) {
                stale++; // 같은 버전(재전달) 또는 더 최신 버전이 이미 있음(늦은 도착)
            } else if (item.status() == 429 || item.status() >= 500) {
                retryable.add(item.id() + ": " + item.error().type());
            } else {
                rejected.add(item.id() + ": " + item.error().type() + " " + item.error().reason());
            }
        }
        return new BulkOutcome(indexed, stale, retryable, rejected);
    }

    /** 발생 시각 [from, to) 안의 이력을 최신 발생 순(같은 시각이면 incidentId 오름차순)으로 한 페이지 읽는다. */
    public HistoryPage search(Instant from, Instant to, IncidentType type, int page, int size) throws IOException {
        try {
            SearchResponse<IncidentHistoryDocument> response = client.search(s -> s
                            .index(properties.index())
                            .query(q -> q.bool(b -> {
                                b.filter(f -> f.range(r -> r.date(d -> d
                                        .field("occurredAt")
                                        .gte(from.toString())
                                        .lt(to.toString()))));
                                if (type != null) {
                                    b.filter(f -> f.term(t -> t.field("type").value(type.name())));
                                }
                                return b;
                            }))
                            .sort(so -> so.field(f -> f.field("occurredAt").order(SortOrder.Desc)))
                            .sort(so -> so.field(f -> f.field("incidentId").order(SortOrder.Asc)))
                            .from(page * size)
                            .size(size)
                            .trackTotalHits(t -> t.enabled(true)),
                    IncidentHistoryDocument.class);
            long total = response.hits().total() == null ? 0 : response.hits().total().value();
            List<IncidentHistoryDocument> documents = response.hits().hits().stream()
                    .map(Hit::source)
                    .filter(Objects::nonNull)
                    .toList();
            return new HistoryPage(documents, total);
        } catch (ElasticsearchException e) {
            if (isIndexNotFound(e)) {
                return new HistoryPage(List.of(), 0); // 아직 한 건도 색인되지 않음
            }
            throw e;
        }
    }

    public Optional<IncidentHistoryDocument> findById(String incidentId) throws IOException {
        try {
            GetResponse<IncidentHistoryDocument> response =
                    client.get(g -> g.index(properties.index()).id(incidentId), IncidentHistoryDocument.class);
            return response.found() ? Optional.ofNullable(response.source()) : Optional.empty();
        } catch (ElasticsearchException e) {
            if (isIndexNotFound(e)) {
                return Optional.empty();
            }
            throw e;
        }
    }

    private static boolean isIndexNotFound(ElasticsearchException e) {
        return "index_not_found_exception".equals(errorType(e));
    }

    private static String errorType(ElasticsearchException e) {
        return e.error() == null ? null : e.error().type();
    }

    /**
     * @param retryable 다시 시도하면 될 수 있는 실패(429·5xx)
     * @param rejected  다시 시도해도 실패할 항목(매핑 오류 등)
     */
    public record BulkOutcome(int indexed, int stale, List<String> retryable, List<String> rejected) {
    }

    public record HistoryPage(List<IncidentHistoryDocument> documents, long total) {
    }
}
