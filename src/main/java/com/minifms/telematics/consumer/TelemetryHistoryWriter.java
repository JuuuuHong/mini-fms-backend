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
import com.minifms.common.kafka.EventPublishException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * 이력 경로: telematics-raw-events → 멱등 판별 → ClickHouse 일괄 INSERT.
 *
 * <p>배치를 날짜(파티션)별 INSERT 로 넣는다(ClickHouse 는 작은 INSERT 를 자주 하면 part 가 늘어 병합 부담이 커지고,
 * 한 INSERT 가 파티션 100개를 넘으면 거부한다). 저장한 날짜는 바로 확정하고, 실패하면 남은 선점을 풀고 예외를 던져
 * 에러 핸들러가 같은 배치를 무기한 재시도하게 한다.
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

        // 날짜(UTC = ClickHouse 파티션)별로 나눠 넣는다. INSERT 한 번이 파티션 하나만 건드리고,
        // 저장한 날짜는 바로 DONE 으로 확정해, 뒤 날짜가 실패해 배치가 재처리돼도 앞 날짜를 다시 넣지 않는다.
        Map<LocalDate, List<TelematicsEvent>> byDay = new LinkedHashMap<>();
        for (int i = 0; i < events.size(); i++) {
            if (claim.status(i) == ClaimStatus.CLAIMED) {
                TelematicsEvent event = events.get(i);
                byDay.computeIfAbsent(utcDay(event), day -> new ArrayList<>()).add(event);
            }
        }
        Set<String> settled = new HashSet<>();
        int inserted = 0;
        for (List<TelematicsEvent> dayEvents : byDay.values()) {
            List<String> dayKeys = dayEvents.stream().map(TelematicsEvent::idempotencyKey).toList();
            try {
                historyRepository.insertBatch(dayEvents);
            } catch (RuntimeException e) {
                // 저장하지 못한 날짜부터의 선점만 푼다. 이 예외로 배치가 다시 오면 그 날짜만 다시 넣는다.
                // DB 접속 장애처럼 복구될 수 있는 오류이므로 DLT 로 보내지 않고 재시도한다(에러 핸들러: 무기한 백오프).
                releaseQuietly(claim, claim.claimedKeys().stream().filter(key -> !settled.contains(key)).toList());
                throw e;
            }
            idempotencyGuard.complete(claim, dayKeys);
            settled.addAll(dayKeys);
            inserted += dayEvents.size();
        }
        metrics.processed(SINK, inserted);
        metrics.duplicates(SINK, (int) claim.count(ClaimStatus.DUPLICATE));

        // DLT 기록이 확인돼야 배치를 끝낸다. 실패하면 예외로 오프셋을 확정하지 않고 다시 처리한다
        // (정상 레코드는 이미 DONE 이라 다시 저장되지 않고, 무효 레코드만 다시 DLT 로 보낸다).
        forwardToDeadLetter(invalidRecords, invalidReasons);
        metrics.invalid(SINK, invalidRecords.size());
    }

    private static LocalDate utcDay(TelematicsEvent event) {
        return Instant.ofEpochMilli(event.timestamp()).atZone(ZoneOffset.UTC).toLocalDate();
    }

    /**
     * 무효 메시지는 원본 그대로 DLT 로 옮겨 나중에 원인을 볼 수 있게 한다(이력 소비자 한 곳에서만 옮긴다).
     * 검증·해석에 실패한 레코드는 다시 처리해도 같은 결과이므로 DLT 로 보낼 근거가 있다.
     * 발행이 확인되지 않으면 예외를 던져 배치의 오프셋을 확정하지 않는다(로그에만 남기고 넘어가지 않는다).
     */
    private void forwardToDeadLetter(List<ConsumerRecord<String, String>> records, List<String> reasons) {
        if (records.isEmpty()) {
            return;
        }
        List<CompletableFuture<SendResult<String, String>>> sends = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, String> source = records.get(i);
            ProducerRecord<String, String> dlt = new ProducerRecord<>(topics.telematicsDlt(), source.key(), source.value());
            dlt.headers()
                    .add("fms-error", reasons.get(i).getBytes(StandardCharsets.UTF_8))
                    .add("fms-source", (source.topic() + "/" + source.partition() + "/" + source.offset())
                            .getBytes(StandardCharsets.UTF_8));
            try {
                sends.add(kafkaTemplate.send(dlt));
            } catch (RuntimeException e) {
                sends.add(CompletableFuture.failedFuture(e));
            }
        }
        try {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                    .get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("DLT 기록 대기 중 인터럽트, 배치를 다시 처리합니다", e, true);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("무효 텔레메트리 " + records.size() + "건의 DLT 기록을 확인하지 못해 배치를 다시 처리합니다",
                    e, true);
        }
        for (int i = 0; i < records.size(); i++) {
            ConsumerRecord<String, String> source = records.get(i);
            log.warn("무효 텔레메트리를 DLT 로 옮김 source={}/{}/{}: {}",
                    source.topic(), source.partition(), source.offset(), reasons.get(i));
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
