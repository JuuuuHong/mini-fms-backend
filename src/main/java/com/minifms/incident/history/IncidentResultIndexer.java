package com.minifms.incident.history;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.KafkaConsumerConfig;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.incident.IncidentProcessingResult;
import com.minifms.incident.history.IncidentHistoryRepository.BulkOutcome;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * c-its-incident-results → Elasticsearch 비동기 색인.
 *
 * <p>Elasticsearch 가 멈추면 bulk 요청이 예외로 끝나고, 에러 핸들러가 같은 배치를 무기한 재시도한다(오프셋을 커밋하지
 * 않으므로 결과를 잃지 않는다). 그동안 접수·경고 발행은 이 소비자와 무관하게 계속되고, 복구되면 밀린 결과를 처리한다.
 * 이미 색인한 결과를 다시 보내도 external 버전 덕분에 409(stale)로 끝나므로 재시도는 안전하다.
 */
@Component
public class IncidentResultIndexer {

    private static final Logger log = LoggerFactory.getLogger(IncidentResultIndexer.class);

    private final IncidentHistoryRepository repository;
    private final ObjectMapper objectMapper;
    private final FmsMetrics metrics;

    public IncidentResultIndexer(IncidentHistoryRepository repository, ObjectMapper objectMapper, FmsMetrics metrics) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @KafkaListener(
            id = "incident-history-indexer",
            groupId = "${fms.consumers.incident-indexer-group:fms-incident-indexer}",
            topics = "${fms.topics.incident-results:c-its-incident-results}",
            containerFactory = KafkaConsumerConfig.INCIDENT_INDEXER_FACTORY)
    public void onBatch(List<ConsumerRecord<String, String>> records) throws IOException {
        // 같은 배치 안에서는 incidentId 별로 가장 큰 버전만 보낸다(나머지는 어차피 409 로 버려진다).
        Map<String, IncidentProcessingResult> latest = new LinkedHashMap<>();
        int malformed = 0;
        for (ConsumerRecord<String, String> record : records) {
            try {
                IncidentProcessingResult result = objectMapper.readValue(record.value(), IncidentProcessingResult.class);
                if (result == null || result.incidentId() == null) {
                    throw new IllegalArgumentException("incidentId 없음");
                }
                latest.merge(result.incidentId(), result, (a, b) -> a.version() >= b.version() ? a : b);
            } catch (JsonProcessingException | IllegalArgumentException e) {
                malformed++;
                log.error("해석할 수 없는 처리 결과를 건너뜁니다 partition={} offset={}: {}",
                        record.partition(), record.offset(), e.getMessage());
            }
        }
        int supersededInBatch = records.size() - malformed - latest.size();
        if (latest.isEmpty()) {
            metrics.incidentHistoryRejected(malformed);
            return;
        }

        BulkOutcome outcome = repository.indexAll(latest.values());
        if (!outcome.retryable().isEmpty()) {
            // 배치 전체를 다시 시도한다. 이미 반영된 항목은 다음 시도에서 409(stale)가 된다.
            throw new IOException("일시적인 색인 실패 " + outcome.retryable().size() + "건: " + outcome.retryable());
        }
        if (!outcome.rejected().isEmpty()) {
            log.error("색인할 수 없는 처리 결과 {}건(매핑·형식 오류): {}", outcome.rejected().size(), outcome.rejected());
        }
        metrics.incidentHistoryIndexed(outcome.indexed());
        metrics.incidentHistoryStale(outcome.stale() + supersededInBatch);
        metrics.incidentHistoryRejected(outcome.rejected().size() + malformed);
    }
}
