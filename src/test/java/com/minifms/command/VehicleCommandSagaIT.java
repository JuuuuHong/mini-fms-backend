package com.minifms.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.VehicleCommand;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.redis.RedisKeys;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 원격 제어 사가: REQUESTED → TRANSMITTED → ACKNOWLEDGED, 그리고 TIMEOUT·FAILED 의 롤백·보상 이벤트.
 * 차량 응답은 테스트 프로필의 TCU 시뮬레이터가 한다(T-NORESP-* 무응답, T-REJECT 거절, 그 밖은 성공).
 * 테스트 프로필의 응답 마감은 2초.
 */
class VehicleCommandSagaIT extends IntegrationTestSupport {

    @Autowired
    VehicleLocationRepository locationRepository;
    @Autowired
    VehicleCommandRepository commandRepository;
    @Autowired
    StringRedisTemplate redis;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    TopicProperties topics;

    @Test
    void 차량이_응답하면_ACKNOWLEDGED_로_끝나고_제어_상태가_유지된다() throws Exception {
        String vehicleId = seedVehicle(TestEvents.uniqueId("C"));

        String commandId = requestCommand(vehicleId, "DOOR_LOCK")
                .andExpect(status().isAccepted())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("TRANSMITTED"))
                .andExpect(jsonPath("$.previousValue").value("UNLOCKED"))
                .andExpect(jsonPath("$.targetValue").value("LOCKED"))
                .andReturn().getResponse().getContentAsString()
                .transform(body -> JsonPath.read(body, "$.commandId"));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(commandRepository.find(commandId)).map(VehicleCommand::status).hasValue(CommandStatus.ACKNOWLEDGED));
        assertThat(commandRepository.findControlState(vehicleId).door()).isEqualTo("LOCKED");

        mockMvc.perform(get("/api/v1/vehicles/{id}/commands/{cid}", vehicleId, commandId).with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACKNOWLEDGED"))
                .andExpect(jsonPath("$.rolledBack").value(false));
    }

    @Test
    void 응답이_없으면_TIMEOUT_으로_롤백하고_보상_이벤트를_발행한다() throws Exception {
        String vehicleId = seedVehicle("T-NORESP-1");

        String commandId = requestCommand(vehicleId, "DOOR_LOCK")
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString()
                .transform(body -> JsonPath.read(body, "$.commandId"));
        // 낙관적 선반영: 응답 전에 이미 목표 값
        assertThat(commandRepository.findControlState(vehicleId).door()).isEqualTo("LOCKED");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            VehicleCommand command = commandRepository.find(commandId).orElseThrow();
            assertThat(command.status()).isEqualTo(CommandStatus.TIMEOUT);
            assertThat(command.rolledBack()).isTrue();
            assertThat(command.failureReason()).startsWith("NO_ACK_WITHIN_");
        });
        assertThat(commandRepository.findControlState(vehicleId).door()).isEqualTo("UNLOCKED");

        JsonNode compensation = awaitCompensation(commandId);
        assertThat(compensation.get("failedStatus").asText()).isEqualTo("TIMEOUT");
        assertThat(compensation.get("restoredValue").asText()).isEqualTo("UNLOCKED");
        assertThat(compensation.get("revertedValue").asText()).isEqualTo("LOCKED");

        // 잠금이 풀려 다음 명령을 받을 수 있다.
        requestCommand(vehicleId, "DOOR_LOCK").andExpect(status().isAccepted());
    }

    @Test
    void 차량이_거절하면_FAILED_로_롤백하고_보상_이벤트를_발행한다() throws Exception {
        String vehicleId = seedVehicle("T-REJECT");

        String commandId = requestCommand(vehicleId, "ENGINE_STOP")
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString()
                .transform(body -> JsonPath.read(body, "$.commandId"));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            VehicleCommand command = commandRepository.find(commandId).orElseThrow();
            assertThat(command.status()).isEqualTo(CommandStatus.FAILED);
            assertThat(command.rolledBack()).isTrue();
            assertThat(command.failureReason()).startsWith("REJECTED_BY_VEHICLE");
        });
        assertThat(commandRepository.findControlState(vehicleId).engine()).isEqualTo("ON");
        assertThat(awaitCompensation(commandId).get("failedStatus").asText()).isEqualTo("FAILED");
    }

    @Test
    void 처리_중인_명령이_있으면_409() throws Exception {
        String vehicleId = seedVehicle("T-NORESP-2");

        String first = requestCommand(vehicleId, "DOOR_UNLOCK")
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString()
                .transform(body -> JsonPath.read(body, "$.commandId"));

        requestCommand(vehicleId, "DOOR_LOCK")
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.inFlightCommandId").value(first));
    }

    @Test
    void 텔레메트리를_보낸_적_없는_차량은_404_이고_잘못된_명령은_400() throws Exception {
        requestCommand(TestEvents.uniqueId("NONE"), "DOOR_LOCK").andExpect(status().isNotFound());

        String vehicleId = seedVehicle(TestEvents.uniqueId("C"));
        requestCommand(vehicleId, "SELF_DESTRUCT").andExpect(status().isBadRequest());
    }

    private ResultActions requestCommand(String vehicleId, String type) throws Exception {
        return mockMvc.perform(post("/api/v1/vehicles/{id}/commands", vehicleId)
                .with(withScopes(Scopes.VEHICLE_CONTROL))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"type\":\"" + type + "\"}"));
    }

    /** 위치를 한 번 보낸 차량으로 만든다. 고정 ID 를 쓰는 차량은 이전 실행의 흔적을 지운다. */
    private String seedVehicle(String vehicleId) {
        redis.delete(List.of(RedisKeys.vehicleShadow(vehicleId), RedisKeys.vehicleCommandLock(vehicleId)));
        long now = System.currentTimeMillis();
        locationRepository.updateIfNewer(TestEvents.northOf(vehicleId, 35.1595, 126.8526, 0, now), now);
        return vehicleId;
    }

    private JsonNode awaitCompensation(String commandId) {
        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.commandCompensations())) {
            List<JsonNode> events = probe.pollUntil(records -> !matching(records, commandId).isEmpty(), Duration.ofSeconds(20))
                    .stream().map(r -> read(r.value())).filter(n -> commandId.equals(n.get("commandId").asText())).toList();
            assertThat(events).as("보상 이벤트").hasSize(1);
            return events.get(0);
        }
    }

    private List<JsonNode> matching(List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records,
                                    String commandId) {
        return records.stream().map(r -> read(r.value()))
                .filter(n -> commandId.equals(n.get("commandId").asText())).toList();
    }

    private JsonNode read(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
