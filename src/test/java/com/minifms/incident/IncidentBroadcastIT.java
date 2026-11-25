package com.minifms.incident;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

/** C-ITS 돌발상황: 반경 2km 안 주행 차량에만 c-its-incident-alerts 를 보내고, 재보고는 다시 보내지 않는다. */
class IncidentBroadcastIT extends IntegrationTestSupport {

    // 다른 테스트와 겹치지 않는 사고 지점(제주)
    static final double LAT = 33.4996;
    static final double LNG = 126.5312;

    @Autowired
    VehicleLocationRepository locationRepository;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    TopicProperties topics;

    @Test
    void 반경_2km_안의_주행_차량에만_경고를_보내고_같은_보고는_한_번만_전파한다() throws Exception {
        String prefix = TestEvents.uniqueId("I");
        String moving = prefix + "-moving";   // 800m, 주행
        String idle = prefix + "-idle";       // 1.5km, 신호 대기(IDLE) — 위험 구간이므로 포함
        String parked = prefix + "-parked";   // 1km, 시동 꺼짐 — 제외
        String far = prefix + "-far";         // 3km — 반경 밖
        long now = System.currentTimeMillis();
        seed(TestEvents.at(moving, LAT, LNG, 800, 0, EngineStatus.ON, 60, now), now);
        seed(TestEvents.at(idle, LAT, LNG, 0, 1_500, EngineStatus.IDLE, 0, now), now);
        seed(TestEvents.at(parked, LAT, LNG, -1_000, 0, EngineStatus.OFF, 0, now), now);
        seed(TestEvents.at(far, LAT, LNG, 3_000, 0, EngineStatus.ON, 50, now), now);

        String incidentId = TestEvents.uniqueId("INC");
        String body = """
                {"incidentId":"%s","type":"ROAD_DEBRIS","latitude":%s,"longitude":%s,
                 "sourceId":"RSU-JEJU-01","occurredAt":%d,"description":"2차로 낙하물"}"""
                .formatted(incidentId, LAT, LNG, now);

        mockMvc.perform(post("/api/v1/incidents").with(withScopes(Scopes.INCIDENT_WRITE))
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.duplicate").value(false))
                .andExpect(jsonPath("$.recipients").value(2))
                .andExpect(jsonPath("$.vehicleIds[0]").value(moving))
                .andExpect(jsonPath("$.vehicleIds[1]").value(idle));

        // RSU 재전송
        mockMvc.perform(post("/api/v1/incidents").with(withScopes(Scopes.INCIDENT_WRITE))
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.duplicate").value(true))
                .andExpect(jsonPath("$.recipients").value(0));

        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.incidentAlerts())) {
            probe.pollUntil(records -> alertsOf(records, incidentId).size() >= 2, Duration.ofSeconds(20));
            List<JsonNode> alerts = alertsOf(probe.pollFor(Duration.ofSeconds(2)), incidentId);

            assertThat(alerts).extracting(a -> a.get("vehicleId").asText()).containsExactlyInAnyOrder(moving, idle);
            JsonNode toMoving = alerts.stream().filter(a -> a.get("vehicleId").asText().equals(moving)).findFirst().orElseThrow();
            assertThat(toMoving.get("type").asText()).isEqualTo("ROAD_DEBRIS");
            assertThat(toMoving.get("distanceMeters").asLong()).isBetween(795L, 805L);
            assertThat(toMoving.get("message").asText()).contains("도로 낙하물");
            assertThat(toMoving.get("expiresAt").asText()).isNotBlank();
        }
    }

    @Test
    void 보고_권한이_없으면_403() throws Exception {
        mockMvc.perform(post("/api/v1/incidents").with(withScopes(Scopes.FLEET_READ))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"incidentId":"X1","type":"ACCIDENT","latitude":33.5,"longitude":126.5,
                                 "sourceId":"RSU-1","occurredAt":1700000000000}"""))
                .andExpect(status().isForbidden());
    }

    private void seed(com.minifms.telematics.domain.TelematicsEvent event, long now) {
        locationRepository.updateIfNewer(event, now);
    }

    private List<JsonNode> alertsOf(List<ConsumerRecord<String, String>> records, String incidentId) {
        return records.stream()
                .map(r -> {
                    try {
                        return objectMapper.readTree(r.value());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .filter(n -> incidentId.equals(n.get("incidentId").asText()))
                .toList();
    }
}
