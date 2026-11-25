package com.minifms.telematics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.geofence.GeofenceCandidate;
import com.minifms.geofence.GeofenceDetector;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import com.minifms.telematics.consumer.RealtimeLocationRouter;
import com.minifms.telematics.consumer.TelemetryDecoder;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.vehicle.location.LocationUpdate;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;

/**
 * 실시간 경로의 재전달 안전성: 위치를 저장한 직후 처리가 실패해 Kafka 가 같은 메시지를 다시 주어도
 * 지오펜스 알림이 빠지거나 두 번 나가지 않아야 한다.
 *
 * <p>Kafka 재전달은 같은 레코드로 리스너를 다시 부르는 것과 같으므로, 라우터를 직접 두 번 호출해 재현한다.
 */
class RealtimeRedeliveryIT extends IntegrationTestSupport {

    // 테스트 지오펜스 TEST-DC(부산, 반경 1,000m)
    static final double DC_LAT = 35.1796;
    static final double DC_LNG = 129.0756;

    @Autowired
    TelemetryDecoder decoder;
    @Autowired
    IdempotencyGuard idempotencyGuard;
    @Autowired
    GeofenceDetector geofenceDetector;
    @Autowired
    FmsMetrics metrics;
    @Autowired
    Clock clock;
    @Autowired
    StringRedisTemplate redis;
    @Autowired
    LocationProperties locationProperties;
    @Autowired
    VehicleLocationRepository locationRepository;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    TopicProperties topics;

    @Test
    void 위치_저장_직후_실패해도_재전달로_진입_알림이_정확히_한_건_나간다() throws Exception {
        String vehicleId = TestEvents.uniqueId("R");
        FailOnceAfterSave repository = new FailOnceAfterSave(redis, locationProperties);
        RealtimeLocationRouter router =
                new RealtimeLocationRouter(decoder, idempotencyGuard, repository, geofenceDetector, metrics, clock);
        long t = System.currentTimeMillis() - 10_000;
        ConsumerRecord<String, String> outside = record(TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 1_500, t), 0);
        ConsumerRecord<String, String> inside = record(TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 500, t + 1_000), 1);

        router.onBatch(List.of(outside));

        repository.failNextAfterSave();
        assertThatThrownBy(() -> router.onBatch(List.of(inside))).isInstanceOf(BatchListenerFailedException.class);
        router.onBatch(List.of(inside)); // Kafka 재전달

        assertThat(locationRepository.findLatest(vehicleId))
                .hasValueSatisfying(latest -> assertThat(latest.timestamp()).isEqualTo(Instant.ofEpochMilli(t + 1_000)));
        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.geofenceAlerts())) {
            probe.pollUntil(records -> !alertsOf(records, vehicleId).isEmpty(), Duration.ofSeconds(15));
            List<String> transitions = alertsOf(probe.pollFor(Duration.ofSeconds(2)), vehicleId).stream()
                    .map(alert -> alert.get("transition").asText())
                    .toList();
            assertThat(transitions).containsExactly("ENTER");
        }
    }

    private ConsumerRecord<String, String> record(TelematicsEvent event, long offset) throws Exception {
        return new ConsumerRecord<>(topics.telematicsRaw(), 0, offset, event.vehicleId(), objectMapper.writeValueAsString(event));
    }

    private List<JsonNode> alertsOf(List<ConsumerRecord<String, String>> records, String vehicleId) {
        return records.stream()
                .filter(r -> vehicleId.equals(r.key()))
                .map(r -> {
                    try {
                        return objectMapper.readTree(r.value());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .toList();
    }

    /** 위치 저장은 Redis 에 반영된 뒤, 응답 직후 한 번 실패한다(연결 끊김·응답 유실 흉내). */
    static class FailOnceAfterSave extends VehicleLocationRepository {

        private final AtomicBoolean armed = new AtomicBoolean();

        FailOnceAfterSave(StringRedisTemplate redis, LocationProperties properties) {
            super(redis, properties);
        }

        void failNextAfterSave() {
            armed.set(true);
        }

        @Override
        public LocationUpdate applyIfNewer(TelematicsEvent event, long ingestedAtMillis, List<GeofenceCandidate> candidates) {
            LocationUpdate update = super.applyIfNewer(event, ingestedAtMillis, candidates);
            if (armed.compareAndSet(true, false)) {
                throw new RedisSystemException("위치 저장 직후 실패(시험)", new IllegalStateException());
            }
            return update;
        }
    }
}
