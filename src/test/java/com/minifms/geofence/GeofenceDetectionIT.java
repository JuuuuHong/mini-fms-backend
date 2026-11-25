package com.minifms.geofence;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import com.minifms.telematics.domain.TelematicsEvent;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 지오펜스 진입·이탈: 텔레메트리(Kafka) → 실시간 라우터 → 상태 전이(Lua) → 아웃박스 → vehicle-geofence-alerts.
 * 테스트 지오펜스 TEST-DC: 부산(35.1796, 129.0756), 반경 1,000m, 완충 30m.
 */
class GeofenceDetectionIT extends IntegrationTestSupport {

    static final double DC_LAT = 35.1796;
    static final double DC_LNG = 129.0756;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    TopicProperties topics;

    @Test
    void 진입과_이탈을_한_번씩만_알리고_경계_완충_구간과_중복_과거_이벤트는_무시한다() throws Exception {
        String vehicleId = TestEvents.uniqueId("G");
        long t = System.currentTimeMillis() - 30_000;

        send(TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 1_500, t));          // 밖(처음 관측) → 알림 없음
        send(TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 500, t + 1_000));    // 진입 → ENTER
        send(TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 600, t + 2_000));    // 계속 안 → 없음
        send(TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 1_015, t + 3_000));  // 완충 구간(1,000~1,030m) → 상태 유지
        TelematicsEvent exit = TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 1_200, t + 4_000);
        send(exit);                                                             // 이탈 → EXIT
        send(exit);                                                             // 재전송 → 없음
        send(TestEvents.northOf(vehicleId, DC_LAT, DC_LNG, 400, t + 1_500));    // 지연 도착한 과거 좌표 → 없음

        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.geofenceAlerts())) {
            probe.pollUntil(records -> alertsOf(records, vehicleId).size() >= 2, Duration.ofSeconds(30));
            List<JsonNode> alerts = alertsOf(probe.pollFor(Duration.ofSeconds(3)), vehicleId);

            assertThat(alerts).extracting(a -> a.get("transition").asText()).containsExactly("ENTER", "EXIT");
            assertThat(alerts).allSatisfy(a -> {
                assertThat(a.get("geofenceId").asText()).isEqualTo("TEST-DC");
                assertThat(a.get("alertId").asText()).isNotBlank();
            });
            assertThat(alerts.get(0).get("distanceMeters").asLong()).isBetween(495L, 505L);
            assertThat(alerts.get(1).get("distanceMeters").asLong()).isBetween(1_195L, 1_205L);
        }
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

    private void send(TelematicsEvent event) throws Exception {
        kafkaTemplate.send(topics.telematicsRaw(), event.vehicleId(), objectMapper.writeValueAsString(event)).get();
    }
}
