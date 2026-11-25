package com.minifms.telematics.consumer;

import com.minifms.common.config.KafkaConsumerConfig;
import com.minifms.common.idempotency.ClaimResult;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.geofence.GeofenceDetector;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

/**
 * 실시간 경로: telematics-raw-events → 멱등 판별 → Redis GEO(최신 위치) → 지오펜스 판정.
 *
 * <p>배치 단위로 받아 멱등키 선점을 Redis 왕복 1회로 끝내고, 메시지마다 위치를 갱신한다.
 * 한 메시지에서 실패하면 그 앞까지는 확정·커밋하고, 그 메시지부터 재전달되게
 * {@link BatchListenerFailedException} 에 위치를 실어 던진다.
 */
@Component
public class RealtimeLocationRouter {

    static final String SINK = "realtime";
    private static final Logger log = LoggerFactory.getLogger(RealtimeLocationRouter.class);

    private final TelemetryDecoder decoder;
    private final IdempotencyGuard idempotencyGuard;
    private final VehicleLocationRepository locationRepository;
    private final GeofenceDetector geofenceDetector;
    private final FmsMetrics metrics;
    private final Clock clock;

    public RealtimeLocationRouter(TelemetryDecoder decoder, IdempotencyGuard idempotencyGuard,
                                  VehicleLocationRepository locationRepository, GeofenceDetector geofenceDetector,
                                  FmsMetrics metrics, Clock clock) {
        this.decoder = decoder;
        this.idempotencyGuard = idempotencyGuard;
        this.locationRepository = locationRepository;
        this.geofenceDetector = geofenceDetector;
        this.metrics = metrics;
        this.clock = clock;
    }

    @KafkaListener(
            id = "realtime-location-router",
            groupId = "${fms.consumers.realtime-group:fms-realtime-router}",
            topics = "${fms.topics.telematics-raw:telematics-raw-events}",
            containerFactory = KafkaConsumerConfig.REALTIME_FACTORY,
            concurrency = "${fms.consumers.realtime-concurrency:3}")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        List<TelematicsEvent> events = new ArrayList<>();
        List<ConsumerRecord<String, String>> eventRecords = new ArrayList<>();
        int invalid = 0;
        for (ConsumerRecord<String, String> record : records) {
            DecodedTelemetry decoded = decoder.decode(record.value());
            if (decoded.isValid()) {
                events.add(decoded.event());
                eventRecords.add(record);
            } else {
                invalid++;
                log.warn("무효 텔레메트리 건너뜀 partition={} offset={}: {}", record.partition(), record.offset(), decoded.error());
            }
        }
        metrics.invalid(SINK, invalid);
        if (events.isEmpty()) {
            return;
        }

        List<String> keys = events.stream().map(TelematicsEvent::idempotencyKey).toList();
        ClaimResult claim = idempotencyGuard.claim(SINK, keys);
        Set<String> completed = new LinkedHashSet<>();
        int duplicates = 0;

        for (int i = 0; i < events.size(); i++) {
            ConsumerRecord<String, String> record = eventRecords.get(i);
            switch (claim.status(i)) {
                case DUPLICATE -> duplicates++;
                case IN_FLIGHT -> {
                    metrics.duplicates(SINK, duplicates);
                    settle(claim, completed);
                    throw new BatchListenerFailedException(
                            "다른 처리 시도가 선점 중인 메시지(리스 만료 뒤 재시도): " + keys.get(i), record);
                }
                case CLAIMED -> {
                    try {
                        route(events.get(i));
                        completed.add(keys.get(i));
                    } catch (RuntimeException e) {
                        metrics.duplicates(SINK, duplicates);
                        settle(claim, completed);
                        throw new BatchListenerFailedException("실시간 라우팅 실패: " + keys.get(i), e, record);
                    }
                }
            }
        }
        idempotencyGuard.complete(claim, completed);
        metrics.duplicates(SINK, duplicates);
        metrics.processed(SINK, completed.size());
    }

    private void route(TelematicsEvent event) {
        boolean advanced = locationRepository.updateIfNewer(event, clock.millis());
        if (advanced) {
            geofenceDetector.evaluate(event);
        } else {
            // 더 최신 위치가 이미 반영돼 있다(지연 도착). 최신 위치·지오펜스 상태를 과거로 되돌리지 않는다.
            metrics.outOfOrder();
        }
    }

    /** 처리한 키는 DONE 으로 확정하고, 선점만 하고 처리하지 못한 키는 풀어 재전달 때 다시 선점되게 한다. */
    private void settle(ClaimResult claim, Set<String> completed) {
        try {
            idempotencyGuard.complete(claim, completed);
            Set<String> unprocessed = new LinkedHashSet<>(claim.claimedKeys());
            unprocessed.removeAll(completed);
            idempotencyGuard.release(claim, unprocessed);
        } catch (RuntimeException e) {
            log.warn("멱등키 정리 실패(선점은 리스 만료로 풀립니다): {}", e.getMessage());
        }
    }
}
