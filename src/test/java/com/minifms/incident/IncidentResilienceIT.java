package com.minifms.incident;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.IncidentProperties;
import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.kafka.EventPublishException;
import com.minifms.common.kafka.KafkaEventPublisher;
import com.minifms.common.redis.RedisKeys;
import com.minifms.common.web.ConflictException;
import com.minifms.incident.IncidentBroadcastService.BroadcastResult;
import com.minifms.incident.history.IncidentHistoryView;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.TestEvents;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MvcResult;

/**
 * 돌발상황 전파의 장애 동작: 브로커 장애 때 요청 시간이 대상 수에 비례해 늘지 않는지, 선점을 잃은 처리가
 * 이어받은 처리의 결과를 덮지 않는지, 대상 수 상한이 시동 꺼짐 차량을 거른 뒤에 적용되는지.
 */
class IncidentResilienceIT extends IntegrationTestSupport {

    @Autowired
    VehicleLocationRepository locationRepository;
    @Autowired
    IdempotencyGuard idempotencyGuard;
    @Autowired
    IncidentResultRecorder resultRecorder;
    @Autowired
    IncidentProperties incidentProperties;
    @Autowired
    LocationProperties locationProperties;
    @Autowired
    TopicProperties topics;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    StringRedisTemplate redis;
    @Autowired
    Clock clock;

    @Test
    void 브로커에_닿지_못하면_첫_실패에서_발행을_멈추고_FAILED_를_남긴다() throws Exception {
        double lat = 35.5384; // 울산 부근
        double lng = 129.3114;
        seed(TestEvents.uniqueId("KO"), lat, lng, 5, EngineStatus.ON);
        AtomicInteger sends = new AtomicInteger();
        // max.block.ms 동안 메타데이터를 기다리다 실패하는 send 를 흉내 낸다(1초 블록 후 실패).
        KafkaEventPublisher brokerDown = new KafkaEventPublisher(kafkaTemplate, objectMapper) {
            @Override
            public CompletableFuture<SendResult<String, String>> publish(String topic, String key, Object payload) {
                sends.incrementAndGet();
                sleep(1_000);
                return CompletableFuture.failedFuture(
                        new KafkaException("Send failed", new TimeoutException("Topic not present in metadata after 1000 ms")));
            }
        };
        IncidentBroadcastService service = service(brokerDown, incidentProperties);
        String incidentId = TestEvents.uniqueId("INC");

        long started = System.nanoTime();
        assertThatThrownBy(() -> service.broadcast(report(incidentId, lat, lng))).isInstanceOf(EventPublishException.class);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;

        assertThat(sends.get()).as("첫 즉시 실패 뒤에는 보내지 않는다").isEqualTo(1);
        assertThat(elapsedMs).as("대상 5대 × 1초가 아니라 한 번의 블록으로 끝난다").isLessThan(2_500);
        IncidentHistoryView history = awaitHistory(incidentId);
        assertThat(history.status()).isEqualTo(IncidentDispatchStatus.FAILED);
        assertThat(history.targetCount()).isEqualTo(5);
        assertThat(history.confirmedCount()).isZero();
        assertThat(history.failedCount()).isEqualTo(5);
    }

    @Test
    void 처리_중_선점을_잃으면_결과를_남기지_않고_409() throws Exception {
        double lat = 35.8242; // 전주 부근
        double lng = 127.1480;
        seed(TestEvents.uniqueId("CL"), lat, lng, 2, EngineStatus.ON);
        String incidentId = TestEvents.uniqueId("INC");
        String claimKey = RedisKeys.idempotency("incident", incidentId);
        AtomicBoolean stolen = new AtomicBoolean();
        // 첫 경고를 보내는 순간 선점이 만료되고 RSU 재시도가 같은 사고를 선점한 상황을 만든다.
        KafkaEventPublisher slowProcessing = new KafkaEventPublisher(kafkaTemplate, objectMapper) {
            @Override
            public CompletableFuture<SendResult<String, String>> publish(String topic, String key, Object payload) {
                if (stolen.compareAndSet(false, true)) {
                    redis.opsForValue().set(claimKey, "P:rsu-retry", Duration.ofSeconds(15));
                }
                return super.publish(topic, key, payload);
            }
        };

        assertThatThrownBy(() -> service(slowProcessing, incidentProperties).broadcast(report(incidentId, lat, lng)))
                .isInstanceOf(ConflictException.class);

        assertThat(redis.hasKey(RedisKeys.incidentResultVersion(incidentId))).as("버전을 발급하지 않는다").isFalse();
        assertThat(redis.opsForValue().get(claimKey)).as("이어받은 처리의 선점은 그대로").isEqualTo("P:rsu-retry");
        Thread.sleep(3_000); // 아웃박스 릴레이·색인 소비자가 돌 시간
        MvcResult result = mockMvc.perform(get("/api/v1/incidents/{id}", incidentId).with(withScopes(Scopes.FLEET_READ)))
                .andReturn();
        assertThat(result.getResponse().getStatus()).as("이력에 남지 않는다").isEqualTo(404);
        redis.delete(claimKey);
    }

    @Test
    void 대상_수_상한은_시동_꺼짐_차량을_거른_뒤에_적용한다() {
        double lat = 34.7604; // 여수 부근
        double lng = 127.6622;
        String prefix = TestEvents.uniqueId("MX");
        long now = System.currentTimeMillis();
        // 가장 가까운 차량은 시동 꺼짐, 그 뒤로 주행 차량 3대
        locationRepository.updateIfNewer(TestEvents.at(prefix + "-off", lat, lng, 100, 0, EngineStatus.OFF, 0, now), now);
        locationRepository.updateIfNewer(TestEvents.at(prefix + "-on1", lat, lng, 300, 0, EngineStatus.ON, 40, now), now);
        locationRepository.updateIfNewer(TestEvents.at(prefix + "-on2", lat, lng, 500, 0, EngineStatus.ON, 40, now), now);
        locationRepository.updateIfNewer(TestEvents.at(prefix + "-on3", lat, lng, 700, 0, EngineStatus.ON, 40, now), now);
        IncidentProperties maxTwo = new IncidentProperties(2.0, Duration.ofMinutes(10), 2, Duration.ofSeconds(3));

        BroadcastResult result = service(new KafkaEventPublisher(kafkaTemplate, objectMapper), maxTwo)
                .broadcast(report(TestEvents.uniqueId("INC"), lat, lng));

        assertThat(result.vehicleIds()).containsExactly(prefix + "-on1", prefix + "-on2");
    }

    private IncidentBroadcastService service(KafkaEventPublisher publisher, IncidentProperties properties) {
        return new IncidentBroadcastService(locationRepository, idempotencyGuard, publisher, resultRecorder,
                topics, properties, locationProperties, clock);
    }

    private IncidentReport report(String incidentId, double lat, double lng) {
        return new IncidentReport(incidentId, IncidentType.ACCIDENT, lat, lng, "RSU-TEST-09", System.currentTimeMillis(), "장애 시험");
    }

    private void seed(String prefix, double lat, double lng, int count, EngineStatus engineStatus) {
        long now = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            locationRepository.updateIfNewer(
                    TestEvents.at(prefix + "-" + i, lat, lng, 200.0 * (i + 1), 0, engineStatus, 40, now), now);
        }
    }

    private IncidentHistoryView awaitHistory(String incidentId) {
        AtomicReference<IncidentHistoryView> found = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(300)).until(() -> {
            MvcResult result = mockMvc.perform(get("/api/v1/incidents/{id}", incidentId).with(withScopes(Scopes.FLEET_READ)))
                    .andReturn();
            if (result.getResponse().getStatus() != 200) {
                return false;
            }
            found.set(objectMapper.readValue(result.getResponse().getContentAsString(), IncidentHistoryView.class));
            return true;
        });
        return found.get();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
