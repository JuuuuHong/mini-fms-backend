package com.minifms.incident.history;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.minifms.common.config.properties.IncidentHistoryProperties;
import com.minifms.common.web.InvalidRequestException;
import com.minifms.common.web.NotFoundException;
import com.minifms.incident.IncidentType;
import com.minifms.incident.history.IncidentHistoryRepository.HistoryPage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;

/** 돌발상황 관제 이력 조회. 저장소에 닿지 못하면 {@link HistoryUnavailableException}(503). */
@Service
public class IncidentHistoryService {

    /** Elasticsearch 기본 max_result_window. from + size 가 이 값을 넘으면 검색이 거절된다. */
    private static final int MAX_RESULT_WINDOW = 10_000;

    private final IncidentHistoryRepository repository;
    private final IncidentHistoryProperties properties;

    public IncidentHistoryService(IncidentHistoryRepository repository, IncidentHistoryProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    public IncidentHistoryPage search(Instant from, Instant to, IncidentType type, int page, int size) {
        List<String> violations = new ArrayList<>();
        if (!from.isBefore(to)) {
            violations.add("from 은 to 보다 앞서야 합니다");
        } else if (Duration.between(from, to).compareTo(properties.maxSearchRange()) > 0) {
            violations.add("기간은 최대 " + properties.maxSearchRange().toDays() + "일입니다");
        }
        if (size > properties.maxPageSize()) {
            violations.add("size 는 최대 " + properties.maxPageSize() + "입니다");
        }
        if (((long) page + 1) * size > MAX_RESULT_WINDOW) {
            violations.add("page·size 로 " + MAX_RESULT_WINDOW + "번째 이후는 조회할 수 없습니다. 기간을 좁히세요");
        }
        if (!violations.isEmpty()) {
            throw new InvalidRequestException("검색 조건이 올바르지 않습니다", violations);
        }

        HistoryPage result = call(() -> repository.search(from, to, type, page, size));
        List<IncidentHistoryView> content = result.documents().stream().map(IncidentHistoryView::from).toList();
        int totalPages = (int) ((result.total() + size - 1) / size);
        return new IncidentHistoryPage(content, page, size, result.total(), totalPages);
    }

    public IncidentHistoryView get(String incidentId) {
        return call(() -> repository.findById(incidentId))
                .map(IncidentHistoryView::from)
                .orElseThrow(() -> new NotFoundException("돌발상황 이력이 없습니다(아직 색인 전일 수 있습니다): " + incidentId));
    }

    /**
     * 연결 실패·시간 초과·과부하(429)·서버 오류(5xx)만 "잠시 뒤 다시"(503)로 본다.
     * 그 밖의 Elasticsearch 거절(4xx)은 다시 해도 같으므로 서버 오류(500)로 드러낸다.
     */
    private static <T> T call(SearchCall<T> call) {
        try {
            return call.run();
        } catch (IOException | UncheckedIOException e) {
            throw new HistoryUnavailableException("관제 이력 저장소에 연결할 수 없습니다. 잠시 뒤 다시 시도하세요.", e);
        } catch (ElasticsearchException e) {
            if (e.status() == 429 || e.status() >= 500) {
                throw new HistoryUnavailableException("관제 이력 저장소가 일시적으로 요청을 처리하지 못합니다. 잠시 뒤 다시 시도하세요.", e);
            }
            throw new IllegalStateException("관제 이력 검색 요청이 거절됐습니다: " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface SearchCall<T> {
        T run() throws IOException;
    }

    public record IncidentHistoryPage(List<IncidentHistoryView> content, int page, int size,
                                      long totalElements, int totalPages) {
    }
}
