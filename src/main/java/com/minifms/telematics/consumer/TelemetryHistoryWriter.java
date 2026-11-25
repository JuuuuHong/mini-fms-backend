package com.minifms.telematics.consumer;

import com.minifms.common.config.KafkaConsumerConfig;
import com.minifms.common.config.properties.TelematicsProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.ClaimResult;
import com.minifms.common.idempotency.ClaimStatus;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.telematics.history.TelematicsHistoryRepository;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 이력 경로: telematics-raw-events → 멱등 판별 → ClickHouse 일괄 INSERT.
 *
 * <p>배치 전체를 INSERT 한 번으로 넣는다(ClickHouse 는 작은 INSERT 를 자주 하면 part 가 늘어 병합 부담이 커진다).
 * 실패하면 선점을 모두 풀고 예외를 던져, 에러 핸들러가 같은 배치를 무기한 재시도하게 한다.
 *
 * <p>이중 방어: Redis 멱등 판별을 통과한 중복(윈도우 밖 재전송 등)도 테이블 엔진
 * ReplacingMergeTree(ORDER BY vehicle_id, event_time)가 병합 때 하나로 합친다. 조회는 FINAL 로 한다.
 */
@Component
public class TelemetryHistoryWriter {

    static final String SINK = "history";
    private static final Logger log = LoggerFactory.getLogger(TelemetryHistoryWriter.class);

    private final TelemetryDecoder decoder;
    private final IdempotencyGuard idempotencyGuard;
    private final TelematicsHistoryRepository historyRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TopicProperties topics;
    private final TelematicsProperties properties;
    private final FmsMetrics metrics;

    public TelemetryHistoryWriter(TelemetryDecoder decoder, IdempotencyGuard idempotencyGuard,
                                  TelematicsHistoryRepository historyRepository,
                                  KafkaTemplate<String, String> kafkaTemplate, TopicProperties topics,
                                  TelematicsProperties properties, FmsMetrics metrics) {
        this.decoder = decoder;
        this.idempotencyGuard = idempotencyGuard;
        this.historyRepository = historyRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.properties = properties;
        this.metrics = metrics;
    }

    @KafkaListener(
            id = "telematics-history-writer",
            groupId = "${fms.consumers.history-group:fms-history-writer}",
            topics = "${fms.topics.telematics-raw:telematics-raw-events}",
            containerFactory = KafkaConsumerConfig.HISTORY_FACTORY,
            concurrency = "${fms.consumers.history-concurrency:3}")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        List<TelematicsEvent> events = new ArrayList<>();
        List<ConsumerRecord<String, String>> invalidRecords = new ArrayList<>();
        List<String> invalidReasons = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            DecodedTelemetry decoded = decoder.decode(record.value());
            if (decoded.isValid()) {
                events.add(decoded.event());
            } else {
                invalidRecords.add(record);
                invalidReasons.add(decoded.error());
            }
        }

        ClaimResult claim = idempotencyGuard.claim(SINK, events.stream().map(TelematicsEvent::idempotencyKey).toList());
        if (claim.anyInFlight()) {
            releaseQuietly(claim, claim.claimedKeys());
            throw new IllegalStateException("다른 처리 시도가 선점 중인 메시지가 있어 배치를 재시도합니다(리스 만료 대기)");
        }

        List<TelematicsEvent> toInsert = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            if (claim.status(i) == ClaimStatus.CLAIMED) {
                toInsert.add(events.get(i));
            }
        }
        if (!toInsert.isEmpty()) {
            try {
                historyRepository.insertBatch(toInsert);
            } catch (RuntimeException e) {
                releaseQuietly(claim, claim.claimedKeys());
                throw e;
            }
        }
        idempotencyGuard.complete(claim, claim.claimedKeys());

        forwardToDeadLetter(invalidRecords, invalidReasons);
        metrics.processed(SINK, toInsert.size());
        metrics.duplicates(SINK, (int) claim.count(ClaimStatus.DUPLICATE));
        metrics.invalid(SINK, invalidRecords.size());
    }

    /** 무효 메시지는 원본 그대로 DLT 로 옮겨 나중에 원인을 볼 수 있게 한다(이력 소비자 한 곳에서만 옮긴다). */
    private void forwardToDeadLetter(List<ConsumerRecord<String, String>> records, List<String> reasons) {
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, String> source = records.get(i);
            ProducerRecord<String, String> dlt = new ProducerRecord<>(topics.telematicsDlt(), source.key(), source.value());
            dlt.headers()
                    .add("fms-error", reasons.get(i).getBytes(StandardCharsets.UTF_8))
                    .add("fms-source", (source.topic() + "/" + source.partition() + "/" + source.offset())
                            .getBytes(StandardCharsets.UTF_8));
            try {
                kafkaTemplate.send(dlt).get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
                log.warn("무효 텔레메트리를 DLT 로 옮김 source={}/{}/{}: {}",
                        source.topic(), source.partition(), source.offset(), reasons.get(i));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("DLT 기록 실패(무효 메시지 원본은 로그로만 남김) offset={} value={}", source.offset(), source.value(), e);
            }
        }
    }

    private void releaseQuietly(ClaimResult claim, List<String> keys) {
        try {
            idempotencyGuard.release(claim, keys);
        } catch (RuntimeException e) {
            log.warn("멱등키 선점 해제 실패(리스 만료로 풀립니다): {}", e.getMessage());
        }
    }
}
