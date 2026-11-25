package com.minifms.telematics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.TestEvents;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.telematics.history.TelematicsHistoryRepository;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 수집 파이프라인 전체: Kafka(telematics-raw-events) → [실시간] Redis / [이력] ClickHouse, 그리고 멱등 처리.
 */
class TelematicsPipelineIT extends IntegrationTestSupport {

    // 대전 부근
    static final double LAT = 36.3504;
    static final double LNG = 127.3845;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    TopicProperties topics;
    @Autowired
    TelematicsHistoryRepository historyRepository;
    @Autowired
    VehicleLocationRepository locationRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    FmsMetrics metrics;

    @BeforeEach
    void stopMerges() {
        // 백그라운드 병합(ReplacingMergeTree)이 중복을 지워 "멱등 처리 덕분인지" 가려지지 않도록 병합을 멈춘다.
        jdbcTemplate.execute("SYSTEM STOP MERGES telematics_history");
    }

    @AfterEach
    void startMerges() {
        jdbcTemplate.execute("SYSTEM START MERGES telematics_history");
    }

    @Test
    void 중복_메시지는_Redis_와_ClickHouse_에_한_번만_반영된다() throws Exception {
        String vehicleId = TestEvents.uniqueId("P");
        long t0 = System.currentTimeMillis() - 10_000;
        TelematicsEvent e1 = TestEvents.northOf(vehicleId, LAT, LNG, 100, t0);
        TelematicsEvent e2 = TestEvents.northOf(vehicleId, LAT, LNG, 200, t0 + 1_000);
        TelematicsEvent e3 = TestEvents.northOf(vehicleId, LAT, LNG, 300, t0 + 2_000);
        double historyDuplicatesBefore = metrics.count(FmsMetrics.DUPLICATES, "sink", "history");
        double realtimeDuplicatesBefore = metrics.count(FmsMetrics.DUPLICATES, "sink", "realtime");

        send(e1);
        send(e2);
        send(e2); // 재전송
        send(e3);
        send(e3); // 재전송
        sendRaw(vehicleId, "{not-json"); // 깨진 메시지가 파이프라인을 막지 않아야 한다
        sendRaw(vehicleId, objectMapper.writeValueAsString(
                new TelematicsEvent(vehicleId, 95.0, LNG, 10.0, 50.0, e1.engineStatus(), t0 + 3_000))); // 위도 범위 밖

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(historyRepository.countRaw(vehicleId)).isEqualTo(3);
            assertThat(locationRepository.findLatest(vehicleId))
                    .hasValueSatisfying(latest -> assertThat(latest.timestamp()).isEqualTo(Instant.ofEpochMilli(t0 + 2_000)));
            assertThat(metrics.count(FmsMetrics.DUPLICATES, "sink", "history") - historyDuplicatesBefore).isGreaterThanOrEqualTo(2);
            assertThat(metrics.count(FmsMetrics.DUPLICATES, "sink", "realtime") - realtimeDuplicatesBefore).isGreaterThanOrEqualTo(2);
        });
        // 중복이 늦게 반영되는지 잠시 더 지켜본다.
        Thread.sleep(1_500);
        assertThat(historyRepository.countRaw(vehicleId)).isEqualTo(3);

        // 과거 궤적 API: 측정 시각(ms)이 그대로 왕복해야 한다.
        mockMvc.perform(get("/api/v1/vehicles/{id}/track", vehicleId)
                        .param("from", Instant.ofEpochMilli(t0 - 1_000).toString())
                        .param("to", Instant.ofEpochMilli(t0 + 60_000).toString())
                        .with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(3))
                .andExpect(jsonPath("$.points[0].timestamp").value(Instant.ofEpochMilli(t0).toString()))
                .andExpect(jsonPath("$.points[2].timestamp").value(Instant.ofEpochMilli(t0 + 2_000).toString()));
    }

    @Test
    void 늦게_도착한_과거_이벤트는_이력에는_쌓이고_최신_위치는_바꾸지_않는다() {
        String vehicleId = TestEvents.uniqueId("P");
        long now = System.currentTimeMillis();
        send(TestEvents.northOf(vehicleId, LAT, LNG, 500, now));
        send(TestEvents.northOf(vehicleId, LAT, LNG, 900, now - 5_000)); // 지연 도착

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(historyRepository.countRaw(vehicleId)).isEqualTo(2);
            assertThat(locationRepository.findLatest(vehicleId))
                    .hasValueSatisfying(latest -> assertThat(latest.timestamp()).isEqualTo(Instant.ofEpochMilli(now)));
        });
    }

    @Test
    void 수집_API_로_들어온_데이터가_반경_조회에_나타난다() throws Exception {
        String vehicleId = TestEvents.uniqueId("API");
        TelematicsEvent event = TestEvents.northOf(vehicleId, LAT, LNG, 50, System.currentTimeMillis());

        mockMvc.perform(post("/api/v1/telematics")
                        .with(withScopes(Scopes.TELEMATICS_WRITE))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(1));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/api/v1/vehicles/nearby")
                                .param("lat", String.valueOf(LAT)).param("lng", String.valueOf(LNG)).param("radiusKm", "0.5")
                                .with(withScopes(Scopes.FLEET_READ)))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.vehicles[*].vehicleId", hasItem(vehicleId))));
    }

    @Test
    void 수집_API_검증_실패는_400() throws Exception {
        String body = """
                {"vehicleId":"V:bad","latitude":95.0,"longitude":127.0,"speed":10,
                 "batterySoc":50,"engineStatus":"ON","timestamp":1700000000000}""";
        mockMvc.perform(post("/api/v1/telematics")
                        .with(withScopes(Scopes.TELEMATICS_WRITE))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());

        // 서버 시각보다 한참 앞선 timestamp(단말 시계 오류)
        TelematicsEvent future = TestEvents.northOf(TestEvents.uniqueId("API"), LAT, LNG, 0,
                System.currentTimeMillis() + Duration.ofHours(1).toMillis());
        mockMvc.perform(post("/api/v1/telematics")
                        .with(withScopes(Scopes.TELEMATICS_WRITE))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(future)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.violations[0]").exists());
    }

    private void send(TelematicsEvent event) {
        try {
            sendRaw(event.vehicleId(), objectMapper.writeValueAsString(event));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void sendRaw(String key, String value) {
        try {
            kafkaTemplate.send(topics.telematicsRaw(), key, value).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
