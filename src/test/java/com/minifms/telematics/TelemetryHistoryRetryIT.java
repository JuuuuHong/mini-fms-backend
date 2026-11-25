package com.minifms.telematics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.TelematicsProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.kafka.EventPublishException;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import com.minifms.telematics.consumer.TelemetryDecoder;
import com.minifms.telematics.consumer.TelemetryHistoryWriter;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.telematics.history.TelematicsHistoryRepository;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * 이력 기록기의 재처리 안전성. Kafka 재전달은 같은 레코드로 리스너를 다시 부르는 것과 같으므로 직접 두 번 부른다.
 * ClickHouse 백그라운드 병합을 멈춰, 중복이 "병합으로 사라진 것"이 아니라 "애초에 두 번 저장되지 않은 것"임을 확인한다.
 */
class TelemetryHistoryRetryIT extends IntegrationTestSupport {

    static final double LAT = 36.6424; // 청주 부근
    static final double LNG = 127.4890;
    static final long DAY = Duration.ofDays(1).toMillis();

    @Autowired
    TelemetryDecoder decoder;
    @Autowired
    IdempotencyGuard idempotencyGuard;
    @Autowired
    TelematicsHistoryRepository historyRepository;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    TopicProperties topics;
    @Autowired
    TelematicsProperties telematicsProperties;
    @Autowired
    FmsMetrics metrics;
    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void stopMerges() {
        jdbcTemplate.execute("SYSTEM STOP MERGES telematics_history");
    }

    @AfterEach
    void startMerges() {
        jdbcTemplate.execute("SYSTEM START MERGES telematics_history");
    }

    @Test
    void 여러_날짜_중_일부_저장에_실패한_뒤_재처리해도_이미_저장한_날짜는_다시_저장되지_않는다() throws Exception {
        String vehicleId = TestEvents.uniqueId("D");
        long now = System.currentTimeMillis();
        // 사흘(UTC 날짜 3개)에 걸친 6건: 그제 2건, 어제 2건, 오늘 2건
        List<TelematicsEvent> events = List.of(
                TestEvents.northOf(vehicleId, LAT, LNG, 10, now - 2 * DAY),
                TestEvents.northOf(vehicleId, LAT, LNG, 20, now - 2 * DAY + 1_000),
                TestEvents.northOf(vehicleId, LAT, LNG, 30, now - DAY),
                TestEvents.northOf(vehicleId, LAT, LNG, 40, now - DAY + 1_000),
                TestEvents.northOf(vehicleId, LAT, LNG, 50, now - 10_000),
                TestEvents.northOf(vehicleId, LAT, LNG, 60, now - 9_000));
        LocalDate yesterday = Instant.ofEpochMilli(now - DAY).atZone(ZoneOffset.UTC).toLocalDate();
        FailOnceForDay repository = new FailOnceForDay(jdbcTemplate, yesterday);
        TelemetryHistoryWriter writer = writer(repository, kafkaTemplate);
        List<ConsumerRecord<String, String>> batch = records(events);

        // 1차: 그제 저장 → 어제 저장 실패(DB 접속 장애 흉내) → 예외(오프셋 미확정)
        assertThatThrownBy(() -> writer.onBatch(batch)).isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(historyRepository.countRaw(vehicleId)).as("실패 전 날짜만 저장").isEqualTo(2);

        // 2차: 같은 배치 재전달 → 어제·오늘만 저장, 그제는 다시 넣지 않는다
        writer.onBatch(batch);
        assertThat(historyRepository.countRaw(vehicleId)).as("병합 전에도 중복 행 없음").isEqualTo(6);

        // 3차: 한 번 더 재전달돼도 그대로
        writer.onBatch(batch);
        assertThat(historyRepository.countRaw(vehicleId)).isEqualTo(6);

        // 궤적 조회에서도 측정 시각마다 한 건
        String body = mockMvc.perform(get("/api/v1/vehicles/{id}/track", vehicleId)
                        .param("from", Instant.ofEpochMilli(now - 3 * DAY).toString())
                        .param("to", Instant.ofEpochMilli(now + 60_000).toString())
                        .with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode track = objectMapper.readTree(body);
        assertThat(track.get("count").asInt()).isEqualTo(6);
        assertThat(track.get("points").findValuesAsText("timestamp"))
                .containsExactlyElementsOf(events.stream().map(e -> Instant.ofEpochMilli(e.timestamp()).toString()).toList());
    }

    @Test
    void DLT_기록이_확인되지_않으면_배치를_다시_처리하고_정상_레코드는_두_번_저장하지_않는다() throws Exception {
        String vehicleId = TestEvents.uniqueId("DLT");
        TelematicsEvent valid = TestEvents.northOf(vehicleId, LAT, LNG, 0, System.currentTimeMillis());
        List<ConsumerRecord<String, String>> batch = List.of(
                new ConsumerRecord<>(topics.telematicsRaw(), 0, 0L, vehicleId, objectMapper.writeValueAsString(valid)),
                new ConsumerRecord<>(topics.telematicsRaw(), 0, 1L, vehicleId, "{broken-json"));

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> dltDown = mock(KafkaTemplate.class);
        when(dltDown.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.<SendResult<String, String>>failedFuture(new KafkaException("DLT 장애(시험)")));

        // DLT 기록을 확인하지 못하면 예외 → 오프셋 미확정(로그만 남기고 넘어가지 않는다)
        assertThatThrownBy(() -> writer(historyRepository, dltDown).onBatch(batch)).isInstanceOf(EventPublishException.class);
        assertThat(historyRepository.countRaw(vehicleId)).isEqualTo(1);

        // 재처리: 정상 레코드는 이미 확정돼 다시 저장하지 않고, 무효 레코드만 DLT 로 간다
        writer(historyRepository, kafkaTemplate).onBatch(batch);
        assertThat(historyRepository.countRaw(vehicleId)).isEqualTo(1);
        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.telematicsDlt())) {
            List<ConsumerRecord<String, String>> dead = probe.pollUntil(
                    records -> records.stream().anyMatch(r -> vehicleId.equals(r.key())), Duration.ofSeconds(15));
            assertThat(dead.stream().filter(r -> vehicleId.equals(r.key())).map(ConsumerRecord::value))
                    .containsExactly("{broken-json");
        }
    }

    private TelemetryHistoryWriter writer(TelematicsHistoryRepository repository, KafkaTemplate<String, String> template) {
        return new TelemetryHistoryWriter(decoder, idempotencyGuard, repository, template, topics, telematicsProperties, metrics);
    }

    private List<ConsumerRecord<String, String>> records(List<TelematicsEvent> events) throws Exception {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            TelematicsEvent event = events.get(i);
            records.add(new ConsumerRecord<>(topics.telematicsRaw(), 0, i, event.vehicleId(), objectMapper.writeValueAsString(event)));
        }
        return records;
    }

    /** 지정한 날짜의 INSERT 를 처음 한 번만 실패시킨다(DB 접속 장애 흉내). */
    static class FailOnceForDay extends TelematicsHistoryRepository {

        private final LocalDate failingDay;
        private final AtomicBoolean failed = new AtomicBoolean();

        FailOnceForDay(JdbcTemplate jdbcTemplate, LocalDate failingDay) {
            super(jdbcTemplate);
            this.failingDay = failingDay;
        }

        @Override
        public void insertBatch(List<TelematicsEvent> events) {
            LocalDate day = Instant.ofEpochMilli(events.get(0).timestamp()).atZone(ZoneOffset.UTC).toLocalDate();
            if (day.equals(failingDay) && failed.compareAndSet(false, true)) {
                throw new DataAccessResourceFailureException("ClickHouse 연결 끊김(시험)");
            }
            super.insertBatch(events);
        }
    }
}
