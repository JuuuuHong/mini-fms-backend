package com.minifms.incident;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.IncidentProperties;
import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.kafka.EventPublishException;
import com.minifms.common.kafka.KafkaEventPublisher;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.incident.history.IncidentHistoryView;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 돌발상황 관제 이력: 접수 → 처리 결과(c-its-incident-results) → Elasticsearch 비동기 색인 → 검색 API.
 * 다른 테스트의 보고와 섞이지 않도록 테스트마다 다른 발생 시각대·좌표를 쓴다.
 */
class IncidentHistoryIT extends IntegrationTestSupport {

    /** 주변에 차량이 없는 바다 위 좌표 → 대상 0대 */
    static final double OCEAN_LAT = 10.0;
    static final double OCEAN_LNG = 150.0;

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    TopicProperties topics;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
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
    Clock clock;
    @Autowired
    FmsMetrics metrics;
    @Autowired
    ElasticsearchClient elasticsearchClient;

    @Test
    void 기간과_유형으로_최신_발생_순_페이지를_돌려주고_같은_시각은_incidentId_순으로_고정된다() throws Exception {
        String p = TestEvents.uniqueId("H");
        Instant base = Instant.parse("2001-03-01T00:00:00Z");
        postIncident(p + "-a", IncidentType.ROAD_DEBRIS, OCEAN_LAT, OCEAN_LNG, base.plusSeconds(60)).andExpect(status().isAccepted());
        postIncident(p + "-b", IncidentType.ACCIDENT, OCEAN_LAT, OCEAN_LNG, base.plusSeconds(120)).andExpect(status().isAccepted());
        postIncident(p + "-d", IncidentType.ROAD_DEBRIS, OCEAN_LAT, OCEAN_LNG, base.plusSeconds(180)).andExpect(status().isAccepted());
        postIncident(p + "-c", IncidentType.ROAD_DEBRIS, OCEAN_LAT, OCEAN_LNG, base.plusSeconds(180)).andExpect(status().isAccepted());
        postIncident(p + "-late", IncidentType.ROAD_DEBRIS, OCEAN_LAT, OCEAN_LNG, base.plusSeconds(600)).andExpect(status().isAccepted());
        postIncident(p + "-early", IncidentType.ROAD_DEBRIS, OCEAN_LAT, OCEAN_LNG, base.minusSeconds(60)).andExpect(status().isAccepted());

        // 비동기 색인이 끝날 때까지(여섯 건 모두 검색될 때까지) 기다린다.
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(search(base.minusSeconds(3600), base.plusSeconds(3600), null, 0, 20).get("totalElements").asLong())
                        .isEqualTo(6));

        String from = base.toString();
        String to = base.plusSeconds(300).toString(); // [00:00, 00:05) → late·early 제외

        JsonNode all = search(base, base.plusSeconds(300), null, 0, 20);
        assertThat(ids(all)).containsExactly(p + "-c", p + "-d", p + "-b", p + "-a");
        assertThat(all.get("totalElements").asLong()).isEqualTo(4);

        JsonNode debris = search(base, base.plusSeconds(300), IncidentType.ROAD_DEBRIS, 0, 20);
        assertThat(ids(debris)).containsExactly(p + "-c", p + "-d", p + "-a");

        JsonNode page0 = search(base, base.plusSeconds(300), IncidentType.ROAD_DEBRIS, 0, 2);
        JsonNode page1 = search(base, base.plusSeconds(300), IncidentType.ROAD_DEBRIS, 1, 2);
        assertThat(ids(page0)).containsExactly(p + "-c", p + "-d");
        assertThat(ids(page1)).containsExactly(p + "-a");
        assertThat(page0.get("totalElements").asLong()).isEqualTo(3);
        assertThat(page0.get("totalPages").asInt()).isEqualTo(2);
        // 같은 조건이면 몇 번을 물어도 같은 순서
        assertThat(ids(search(base, base.plusSeconds(300), IncidentType.ROAD_DEBRIS, 0, 2))).isEqualTo(ids(page0));

        JsonNode first = all.get("content").get(0);
        assertThat(first.get("sourceId").asText()).isEqualTo("RSU-TEST-01");
        assertThat(first.get("type").asText()).isEqualTo("ROAD_DEBRIS");
        assertThat(first.get("occurredAt").asText()).isEqualTo(base.plusSeconds(180).toString());
        assertThat(first.get("latitude").asDouble()).isEqualTo(OCEAN_LAT);

        // 권한: 토큰 없음 401, 조회 권한(fleet:read) 없는 토큰 403
        mockMvc.perform(get("/api/v1/incidents").param("from", from).param("to", to))
                .andExpect(status().isUnauthorized());
        for (String scope : List.of(Scopes.INCIDENT_WRITE, Scopes.TELEMATICS_WRITE, Scopes.VEHICLE_CONTROL)) {
            mockMvc.perform(get("/api/v1/incidents").param("from", from).param("to", to).with(withScopes(scope)))
                    .andExpect(status().isForbidden());
            mockMvc.perform(get("/api/v1/incidents/{id}", p + "-a").with(withScopes(scope)))
                    .andExpect(status().isForbidden());
        }
    }

    @Test
    void 잘못된_기간이나_유형은_400() throws Exception {
        String from = "2001-03-01T00:00:00Z";
        expectBadRequest(from, "2001-03-01T00:00:00Z", null, null);   // from == to
        expectBadRequest(from, "2001-02-28T00:00:00Z", null, null);   // from > to
        expectBadRequest(from, "2001-04-15T00:00:00Z", null, null);   // 31일 초과
        expectBadRequest(from, "2001-03-02T00:00:00Z", "FLYING_CAR", null);
        expectBadRequest("어제", "2001-03-02T00:00:00Z", null, null);
        expectBadRequest(from, "2001-03-02T00:00:00Z", null, "101");  // size 상한 100
        mockMvc.perform(get("/api/v1/incidents").param("from", from).param("to", "2001-03-02T00:00:00Z")
                        .param("page", String.valueOf(Integer.MAX_VALUE)).param("size", "20")
                        .with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isBadRequest());                   // page 정수 오버플로
        mockMvc.perform(get("/api/v1/incidents").param("from", from).with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isBadRequest());                   // to 없음
    }

    @Test
    void 대상이_0대인_보고도_이력에_남는다() throws Exception {
        String incidentId = TestEvents.uniqueId("H0");
        postIncident(incidentId, IncidentType.HARD_BRAKING, OCEAN_LAT, OCEAN_LNG, Instant.now())
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.recipients").value(0));

        IncidentHistoryView history = awaitHistory(incidentId, view -> true);
        assertThat(history.status()).isEqualTo(IncidentDispatchStatus.NO_TARGETS);
        assertThat(history.targetCount()).isZero();
        assertThat(history.confirmedCount()).isZero();
        assertThat(history.failedCount()).isZero();
    }

    @Test
    void 일부_경고_발행에_실패하면_대상_수_확인_건수_상태를_남기고_재시도의_최신_결과가_이력을_대체한다() throws Exception {
        // 통영 부근, 반경 2km 안 주행 차량 3대 중 1대의 경고를 브로커가 거절하게 한다.
        double lat = 34.8546;
        double lng = 128.4331;
        String prefix = TestEvents.uniqueId("HP");
        List<String> vehicles = List.of(prefix + "-1", prefix + "-2", prefix + "-3");
        seedVehicles(vehicles, lat, lng);
        String incidentId = TestEvents.uniqueId("INC");
        long occurredAt = System.currentTimeMillis();

        IncidentBroadcastService flaky = new IncidentBroadcastService(locationRepository, idempotencyGuard,
                new RejectingPublisher(kafkaTemplate, objectMapper, topics.incidentAlerts(), vehicles.get(1)),
                resultRecorder, topics, incidentProperties, locationProperties, clock);
        IncidentReport report = new IncidentReport(incidentId, IncidentType.ACCIDENT, lat, lng, "RSU-TY-01", occurredAt, "3중 추돌");
        assertThatThrownBy(() -> flaky.broadcast(report)).isInstanceOf(EventPublishException.class);

        IncidentHistoryView partial = awaitHistory(incidentId, view -> true);
        assertThat(partial.status()).isEqualTo(IncidentDispatchStatus.PARTIALLY_FAILED);
        assertThat(partial.targetCount()).isEqualTo(3);
        assertThat(partial.confirmedCount()).isEqualTo(2);
        assertThat(partial.failedCount()).isEqualTo(1);

        // RSU 재시도(실제 접수 API, 기존 요청·응답 계약 그대로) → 전부 확인 → 더 최근 결과가 이력을 대체
        postIncident(incidentId, IncidentType.ACCIDENT, lat, lng, Instant.ofEpochMilli(occurredAt))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.duplicate").value(false))
                .andExpect(jsonPath("$.recipients").value(3));
        IncidentHistoryView completed = awaitHistory(incidentId, view -> view.status() == IncidentDispatchStatus.COMPLETED);
        assertThat(completed.confirmedCount()).isEqualTo(3);
        assertThat(completed.failedCount()).isZero();
        assertThat(completed.version()).isGreaterThan(partial.version());

        // ID 조회는 바로 반영되지만 검색은 Elasticsearch refresh(기본 1초) 뒤에 보인다.
        Instant occurred = Instant.ofEpochMilli(occurredAt);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(ids(search(occurred.minusSeconds(60), occurred.plusSeconds(60), IncidentType.ACCIDENT, 0, 100)))
                        .containsOnlyOnce(incidentId));
    }

    @Test
    void Elasticsearch_가_멈춰도_접수와_경고_발행은_계속되고_검색은_503_이며_복구_뒤_밀린_이력을_찾는다() throws Exception {
        double lat = 37.2636; // 수원 부근
        double lng = 127.0286;
        String prefix = TestEvents.uniqueId("HE");
        seedVehicles(List.of(prefix + "-1", prefix + "-2"), lat, lng);
        String incidentId = TestEvents.uniqueId("INC");
        Instant now = Instant.now();

        pauseElasticsearch();
        long pausedAt = System.nanoTime();
        try {
            double indexedBefore = metrics.count(FmsMetrics.INCIDENT_HISTORY_INDEXED);
            postIncident(incidentId, IncidentType.ROAD_DEBRIS, lat, lng, now)
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.recipients").value(2));

            try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.incidentAlerts())) {
                List<JsonNode> alerts = probe.pollUntil(records -> alertCount(records, incidentId) >= 2, Duration.ofSeconds(20))
                        .stream().map(r -> read(r.value())).filter(n -> incidentId.equals(n.get("incidentId").asText())).toList();
                assertThat(alerts).hasSize(2);
            }

            mockMvc.perform(get("/api/v1/incidents")
                            .param("from", now.minusSeconds(3600).toString()).param("to", now.plusSeconds(3600).toString())
                            .with(withScopes(Scopes.FLEET_READ)))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(header().string(HttpHeaders.RETRY_AFTER, "5"));
            mockMvc.perform(get("/api/v1/incidents/{id}", incidentId).with(withScopes(Scopes.FLEET_READ)))
                    .andExpect(status().isServiceUnavailable());

            // 정지를 색인기 재시도 간격 상한(5초)의 몇 배로 유지한다. 그동안 색인은 늘지 않고(실패·재시도 중),
            // 재시도 횟수·시간에 상한이 있었다면 결과를 버렸을 것이므로 아래 "복구 뒤 조회"가 실패한다.
            long remaining = Duration.ofSeconds(15).toNanos() - (System.nanoTime() - pausedAt);
            if (remaining > 0) {
                Thread.sleep(remaining / 1_000_000);
            }
            assertThat(metrics.count(FmsMetrics.INCIDENT_HISTORY_INDEXED)).as("정지 중 색인 없음").isEqualTo(indexedBefore);
        } finally {
            resumeElasticsearch();
        }

        IncidentHistoryView history = awaitHistory(incidentId, view -> true);
        assertThat(history.status()).isEqualTo(IncidentDispatchStatus.COMPLETED);
        assertThat(history.targetCount()).isEqualTo(2);
        assertThat(history.confirmedCount()).isEqualTo(2);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(ids(search(now.minusSeconds(60), now.plusSeconds(60), IncidentType.ROAD_DEBRIS, 0, 100)))
                        .containsOnlyOnce(incidentId));
    }

    @Test
    void 같은_결과의_재전달은_문서를_늘리지_않고_늦게_도착한_이전_결과는_최신_결과를_덮어쓰지_않는다() throws Exception {
        String incidentId = TestEvents.uniqueId("HV");
        String marker = TestEvents.uniqueId("HM");
        Instant occurredAt = Instant.parse("2002-06-01T09:00:00Z");
        IncidentProcessingResult older = result(incidentId, 1_000L, IncidentDispatchStatus.PARTIALLY_FAILED, 3, 2, occurredAt);
        IncidentProcessingResult newer = result(incidentId, 2_000L, IncidentDispatchStatus.COMPLETED, 3, 3, occurredAt);

        // 모두 같은 파티션에 싣는다. 소비자는 파티션 안 순서대로 처리하므로, 뒤에 보낸 표지가 색인되면 앞의 결과도 처리가 끝난 것이다.
        // 최신 결과가 먼저 색인된 뒤에 이전 결과·재전달을 따로 보내, 소비자의 배치 내 정리가 아니라
        // Elasticsearch 의 external 버전 검사가 막는지를 확인한다.
        sendResult(newer);
        awaitHistory(incidentId, view -> view.version() == 2_000L);

        sendResult(older); // 늦게 도착한 이전 결과
        sendResult(result(marker + "-1", 1L, IncidentDispatchStatus.NO_TARGETS, 0, 0, occurredAt.plusSeconds(1)));
        awaitHistory(marker + "-1", view -> true);
        IncidentHistoryView afterLate = awaitHistory(incidentId, view -> true);
        assertThat(afterLate.version()).as("늦게 도착한 이전 결과가 최신 결과를 덮어쓰면 안 된다").isEqualTo(2_000L);
        assertThat(afterLate.status()).isEqualTo(IncidentDispatchStatus.COMPLETED);

        sendResult(newer); // 같은 결과의 재전달
        sendResult(result(marker + "-2", 1L, IncidentDispatchStatus.NO_TARGETS, 0, 0, occurredAt.plusSeconds(2)));
        awaitHistory(marker + "-2", view -> true);

        IncidentHistoryView history = awaitHistory(incidentId, view -> true);
        assertThat(history.version()).isEqualTo(2_000L);
        assertThat(history.status()).isEqualTo(IncidentDispatchStatus.COMPLETED);
        assertThat(history.confirmedCount()).isEqualTo(3);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            JsonNode page = search(occurredAt.minusSeconds(60), occurredAt.plusSeconds(60), null, 0, 100);
            // 문서 ID 가 incidentId 라 한 건인 것은 구조상 보장된다. 이 테스트의 핵심 증명은 위의 "버전 2000 유지"다.
            assertThat(ids(page)).containsOnlyOnce(incidentId).contains(marker + "-1", marker + "-2");
        });
    }

    @Test
    void 운영_중_색인이_사라져도_매핑을_지정해_다시_만들고_검색이_계속_된다() throws Exception {
        Instant now = Instant.now();
        String before = TestEvents.uniqueId("HX");
        postIncident(before, IncidentType.HARD_BRAKING, OCEAN_LAT, OCEAN_LNG, now).andExpect(status().isAccepted());
        awaitHistory(before, view -> true);

        // 볼륨 없는 재생성·운영자 삭제 등으로 실제 색인과 별칭이 사라진다.
        elasticsearchClient.indices().delete(d -> d.index("c-its-incidents-v1"));

        String after = TestEvents.uniqueId("HX");
        postIncident(after, IncidentType.HARD_BRAKING, OCEAN_LAT, OCEAN_LNG, now.plusSeconds(1)).andExpect(status().isAccepted());
        awaitHistory(after, view -> true);

        // 동적 매핑으로 자동 생성되지 않고, strict 매핑의 실제 색인 + 별칭으로 다시 만들어졌다.
        var mappings = elasticsearchClient.indices().getMapping(m -> m.index("c-its-incidents")).result();
        assertThat(mappings).containsOnlyKeys("c-its-incidents-v1");
        var properties = mappings.get("c-its-incidents-v1").mappings().properties();
        assertThat(properties.get("type").isKeyword()).isTrue();
        assertThat(properties.get("incidentId").isKeyword()).isTrue();
        assertThat(properties.get("location").isGeoPoint()).isTrue();
        // 정렬(incidentId)·유형 필터 검색이 정상 동작한다.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(ids(search(now.minusSeconds(60), now.plusSeconds(60), IncidentType.HARD_BRAKING, 0, 100)))
                        .contains(after));
    }

    // ---- 도우미 ----

    private ResultActions postIncident(String incidentId, IncidentType type, double lat, double lng, Instant occurredAt)
            throws Exception {
        String body = """
                {"incidentId":"%s","type":"%s","latitude":%s,"longitude":%s,
                 "sourceId":"RSU-TEST-01","occurredAt":%d,"description":"시험 보고"}"""
                .formatted(incidentId, type.name(), lat, lng, occurredAt.toEpochMilli());
        return mockMvc.perform(post("/api/v1/incidents").with(withScopes(Scopes.INCIDENT_WRITE))
                .contentType(MediaType.APPLICATION_JSON).content(body));
    }

    private JsonNode search(Instant from, Instant to, IncidentType type, int page, int size) throws Exception {
        var request = get("/api/v1/incidents")
                .param("from", from.toString()).param("to", to.toString())
                .param("page", String.valueOf(page)).param("size", String.valueOf(size))
                .with(withScopes(Scopes.FLEET_READ));
        if (type != null) {
            request.param("type", type.name());
        }
        MvcResult result = mockMvc.perform(request).andExpect(status().isOk()).andReturn();
        return objectMapper.readTree(result.getResponse().getContentAsString());
    }

    private void expectBadRequest(String from, String to, String type, String size) throws Exception {
        var request = get("/api/v1/incidents").param("from", from).param("to", to).with(withScopes(Scopes.FLEET_READ));
        if (type != null) {
            request.param("type", type);
        }
        if (size != null) {
            request.param("size", size);
        }
        mockMvc.perform(request).andExpect(status().isBadRequest());
    }

    /** 색인될 때까지(그리고 조건을 만족할 때까지) 상세 API 를 다시 읽는다. */
    private IncidentHistoryView awaitHistory(String incidentId, Predicate<IncidentHistoryView> condition) {
        AtomicReference<IncidentHistoryView> found = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(300)).until(() -> {
            MvcResult result = mockMvc.perform(get("/api/v1/incidents/{id}", incidentId).with(withScopes(Scopes.FLEET_READ)))
                    .andReturn();
            if (result.getResponse().getStatus() != 200) {
                return false;
            }
            IncidentHistoryView view = objectMapper.readValue(result.getResponse().getContentAsString(), IncidentHistoryView.class);
            found.set(view);
            return condition.test(view);
        });
        return found.get();
    }

    private List<String> ids(JsonNode page) {
        return page.get("content").findValuesAsText("incidentId");
    }

    private void seedVehicles(List<String> vehicleIds, double lat, double lng) {
        long now = System.currentTimeMillis();
        for (int i = 0; i < vehicleIds.size(); i++) {
            locationRepository.updateIfNewer(
                    TestEvents.at(vehicleIds.get(i), lat, lng, 300.0 * (i + 1), 0, EngineStatus.ON, 50, now), now);
        }
    }

    private IncidentProcessingResult result(String incidentId, long version, IncidentDispatchStatus status,
                                            int targets, int confirmed, Instant occurredAt) {
        return new IncidentProcessingResult(incidentId, version, IncidentType.ACCIDENT, "RSU-TEST-02", 35.0, 128.0,
                occurredAt, "버전 시험", status, targets, confirmed, targets - confirmed, Instant.now());
    }

    private void sendResult(IncidentProcessingResult result) throws Exception {
        kafkaTemplate.send(new ProducerRecord<>(topics.incidentResults(), 0, result.incidentId(),
                objectMapper.writeValueAsString(result))).get();
    }

    private long alertCount(List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records, String incidentId) {
        return records.stream().map(r -> read(r.value())).filter(n -> incidentId.equals(n.get("incidentId").asText())).count();
    }

    private JsonNode read(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 특정 차량에 가는 경고만 브로커가 거절한 것처럼 실패시킨다. */
    static class RejectingPublisher extends KafkaEventPublisher {

        private final String rejectedTopic;
        private final String rejectedVehicleId;

        RejectingPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper,
                           String rejectedTopic, String rejectedVehicleId) {
            super(kafkaTemplate, objectMapper);
            this.rejectedTopic = rejectedTopic;
            this.rejectedVehicleId = rejectedVehicleId;
        }

        @Override
        public CompletableFuture<SendResult<String, String>> publish(String topic, String key, Object payload) {
            if (rejectedTopic.equals(topic) && rejectedVehicleId.equals(key)) {
                return CompletableFuture.failedFuture(new KafkaException("브로커가 거절함(시험)"));
            }
            return super.publish(topic, key, payload);
        }
    }
}
