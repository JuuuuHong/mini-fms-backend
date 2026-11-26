package com.minifms.ride;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.command.VehicleCommandRepository;
import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.CommandType;
import com.minifms.command.domain.VehicleCommand;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.redis.RedisKeys;
import com.minifms.ride.device.Device;
import com.minifms.ride.device.DeviceState;
import com.minifms.security.Scopes;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 대여 → 반납(구역 판정·요금) → 결제 요청(아웃박스 → Kafka) → 원격 잠금(LOCKED → AVAILABLE).
 * 잠금 명령 응답은 테스트 프로필의 TCU 시뮬레이터가 한다(T-RIDE-NORESP 무응답, 그 밖은 성공).
 */
class RideSettlementIT extends RideTestSupport {

    @Autowired
    VehicleCommandRepository commandRepository;
    @Autowired
    TopicProperties topics;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    StringRedisTemplate redis;

    @Test
    void 벌금_구역_반납은_요금을_확정하고_결제_요청을_한_번_발행하고_잠금이_확인되면_다시_대여할_수_있다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), PENALTY);
        String userId = TestEvents.uniqueId("user");

        UUID rideId = rent(userId, deviceId);
        assertThat(device(deviceId).state()).isEqualTo(DeviceState.IN_USE);
        backdateStart(rideId, Duration.ofMinutes(10).plusSeconds(30));

        returnRequest(userId, rideId)
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.returnInfo.zoneId").value("T-PENALTY"))
                .andExpect(jsonPath("$.returnInfo.zoneType").value("PENALTY_ZONE"))
                .andExpect(jsonPath("$.fare.unlockFee").value(1_000))
                .andExpect(jsonPath("$.fare.ridingMinutes").value(11))
                .andExpect(jsonPath("$.fare.ridingFee").value(1_650))
                .andExpect(jsonPath("$.fare.zoneAdjustment").value(10_000))
                .andExpect(jsonPath("$.fare.subscriptionDiscount").value(0))
                .andExpect(jsonPath("$.fare.total").value(12_650));
        assertThat(paymentOutboxRows(rideId)).isEqualTo(1);

        // 결제 요청: 아웃박스 → Kafka(키 = rideId), 라이드당 한 건
        JsonNode payment = awaitPaymentRequest(rideId);
        assertThat(payment.get("amount").asLong()).isEqualTo(12_650);
        assertThat(payment.get("currency").asText()).isEqualTo("KRW");
        assertThat(payment.get("idempotencyKey").asText()).isEqualTo("ride-payment:" + rideId);
        assertThat(payment.get("userId").asText()).isEqualTo(userId);
        assertThat(payment.get("zoneType").asText()).isEqualTo("PENALTY_ZONE");
        assertThat(payment.get("fare").get("zoneAdjustment").asLong()).isEqualTo(10_000);
        await().atMost(Duration.ofSeconds(5)).until(() -> rideJdbcClient
                .sql("SELECT published_at FROM payment_outbox WHERE ride_id = :id").param("id", rideId)
                .query(Timestamp.class).optional().isPresent());

        // 원격 잠금: LOCKED → DOOR_LOCK 명령 ACK → AVAILABLE
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(device(deviceId).state()).isEqualTo(DeviceState.AVAILABLE));
        assertThat(commandRepository.findControlState(deviceId).door()).isEqualTo("LOCKED");

        // 다시 대여할 수 있다
        rent(TestEvents.uniqueId("user"), deviceId);
    }

    @Test
    void 주차_권장_구역과_구독권은_할인하고_대여_시점의_구독권을_쓴다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), DISCOUNT);
        String userId = TestEvents.uniqueId("user");
        Instant now = Instant.now();
        subscribe(userId, "PASS_30", now.minus(Duration.ofDays(1)), now.plus(Duration.ofDays(29)));

        UUID rideId = rent(userId, deviceId);
        // 대여 뒤 구독권이 끝나도 대여할 때의 조건으로 정산한다.
        rideJdbcClient.sql("UPDATE subscriptions SET ends_at = :past WHERE user_id = :userId")
                .param("past", Timestamp.from(now.minusSeconds(1))).param("userId", userId).update();
        backdateStart(rideId, Duration.ofMinutes(4).plusSeconds(10));

        // 주행 5분 × 150 = 750, 구독권 30% = 225, 주차 권장 −500 → 1,000 + 750 − 500 − 225 = 1,025
        returnRequest(userId, rideId)
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tariff.subscriptionPlan").value("PASS_30"))
                .andExpect(jsonPath("$.returnInfo.zoneType").value("DISCOUNT_ZONE"))
                .andExpect(jsonPath("$.fare.ridingMinutes").value(5))
                .andExpect(jsonPath("$.fare.subscriptionDiscount").value(225))
                .andExpect(jsonPath("$.fare.zoneAdjustment").value(-500))
                .andExpect(jsonPath("$.fare.total").value(1_025));
    }

    @Test
    void 반납_불가_구역에서는_거절하고_라이드는_계속되며_옮긴_뒤에는_반납된다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), NO_RETURN);
        String userId = TestEvents.uniqueId("user");
        UUID rideId = rent(userId, deviceId);

        returnRequest(userId, rideId)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("NO_RETURN_ZONE"))
                .andExpect(jsonPath("$.zoneId").value("T-NO-RETURN"));
        assertThat(device(deviceId).state()).isEqualTo(DeviceState.IN_USE);
        assertThat(paymentOutboxRows(rideId)).isZero();
        getRide(userId, rideId).andExpect(jsonPath("$.status").value("IN_PROGRESS"));

        moveTo(deviceId, SERVICE, System.currentTimeMillis() + 1);
        returnRequest(userId, rideId)
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.returnInfo.zoneType").value("SERVICE_ZONE"))
                .andExpect(jsonPath("$.fare.zoneAdjustment").value(0));
    }

    @Test
    void 서비스_지역_밖이거나_위치가_오래되면_반납하지_않는다() throws Exception {
        String outsideUser = TestEvents.uniqueId("user");
        UUID outsideRide = rent(outsideUser, registerDeviceAt(TestEvents.uniqueId("KB"), OUTSIDE));
        returnRequest(outsideUser, outsideRide)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("OUT_OF_SERVICE_AREA"));

        // 대여 뒤 단말 보고가 끊겨 마지막 위치가 2분 전이 됐다(허용 30초).
        String staleDevice = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        String staleUser = TestEvents.uniqueId("user");
        UUID staleRide = rent(staleUser, staleDevice);
        redis.opsForHash().put(RedisKeys.vehicleLatest(staleDevice), "timestamp",
                Long.toString(System.currentTimeMillis() - Duration.ofMinutes(2).toMillis()));
        returnRequest(staleUser, staleRide)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("LOCATION_STALE"));
        assertThat(device(staleDevice).state()).isEqualTo(DeviceState.IN_USE);

        for (UUID ride : List.of(outsideRide, staleRide)) {
            assertThat(paymentOutboxRows(ride)).isZero();
        }
    }

    @Test
    void 위치를_보고하지_않거나_보고가_끊긴_기기는_대여하지_않는다() throws Exception {
        // 반납은 단말 위치로만 판정하므로, 위치가 없는 기기를 빌려주면 끝낼 수 없는 라이드가 된다.
        String silentDevice = registerDevice(TestEvents.uniqueId("KB"));
        rentRequest(TestEvents.uniqueId("user"), silentDevice)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("LOCATION_UNAVAILABLE"));

        String staleDevice = registerDevice(TestEvents.uniqueId("KB"));
        moveTo(staleDevice, SERVICE, System.currentTimeMillis() - Duration.ofMinutes(2).toMillis());
        rentRequest(TestEvents.uniqueId("user"), staleDevice)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("LOCATION_STALE"));

        assertThat(device(silentDevice).state()).isEqualTo(DeviceState.AVAILABLE);
        assertThat(device(staleDevice).state()).isEqualTo(DeviceState.AVAILABLE);
    }

    @Test
    void 반납을_다시_요청하면_같은_결과를_돌려주고_결제_요청은_한_건이다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        String userId = TestEvents.uniqueId("user");
        UUID rideId = rent(userId, deviceId);
        backdateStart(rideId, Duration.ofMinutes(2));

        String first = returnRequest(userId, rideId).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        String second = returnRequest(userId, rideId).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(objectMapper.readTree(second)).isEqualTo(objectMapper.readTree(first));
        assertThat(paymentOutboxRows(rideId)).isEqualTo(1);
    }

    @Test
    void 대여_규칙과_권한() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        String other = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        String userId = TestEvents.uniqueId("user");
        UUID rideId = rent(userId, deviceId);

        // 대여 중인 기기·이미 이용 중인 사용자·미등록 기기
        rentRequest(TestEvents.uniqueId("user"), deviceId)
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("DEVICE_NOT_AVAILABLE"))
                .andExpect(jsonPath("$.state").value("IN_USE"));
        rentRequest(userId, other)
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("ACTIVE_RIDE_EXISTS"))
                .andExpect(jsonPath("$.activeRideId").value(rideId.toString()));
        rentRequest(userId, TestEvents.uniqueId("NONE")).andExpect(status().isNotFound());
        rentRequest(userId, "bad id!").andExpect(status().isBadRequest());

        // 남의 라이드는 반납·조회할 수 없고(존재 여부도 숨김), 관제(fleet:read)는 조회할 수 있다.
        String stranger = TestEvents.uniqueId("user");
        returnRequest(stranger, rideId).andExpect(status().isNotFound());
        getRide(stranger, rideId).andExpect(status().isNotFound());
        mockMvc.perform(get("/api/v1/rides/{id}", rideId).with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.userId").value(userId));

        // 조회·제어 토큰으로는 대여 불가, 이용자 토큰으로는 기기 등록 불가, 토큰 없음 401
        mockMvc.perform(post("/api/v1/rides").with(withScopes(Scopes.FLEET_READ, Scopes.VEHICLE_CONTROL))
                        .contentType(MediaType.APPLICATION_JSON).content("{\"deviceId\":\"" + other + "\"}"))
                .andExpect(status().isForbidden());
        mockMvc.perform(put("/api/v1/devices/{id}", TestEvents.uniqueId("KB")).with(asRider(userId)))
                .andExpect(status().isForbidden());
        mockMvc.perform(post("/api/v1/rides")).andExpect(status().isUnauthorized());

        // 기기 등록은 멱등이다(이미 있으면 200 + 현재 상태).
        mockMvc.perform(put("/api/v1/devices/{id}", deviceId).with(withScopes(Scopes.VEHICLE_CONTROL)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("IN_USE"))
                .andExpect(jsonPath("$.currentRideId").value(rideId.toString()));
    }

    @Test
    void 잠금_명령에_응답하지_않는_기기는_LOCKED_로_남아_대여되지_않고_잠금을_다시_보낸다() throws Exception {
        String deviceId = registerDeviceAt("T-RIDE-NORESP", SERVICE);
        String userId = TestEvents.uniqueId("user");
        UUID rideId = rent(userId, deviceId);
        returnRequest(userId, rideId).andExpect(status().isOk());

        // 응답 마감 2초 → TIMEOUT → 백오프 뒤 새 잠금 명령
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(device(deviceId).lockAttempts()).isGreaterThanOrEqualTo(2));
        Device device = device(deviceId);
        assertThat(device.state()).isEqualTo(DeviceState.LOCKED);
        rentRequest(TestEvents.uniqueId("user"), deviceId)
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.state").value("LOCKED"));

        // 두 번째 잠금 명령은 첫 명령이 TIMEOUT 으로 끝난 뒤에만 나간다. 지금 기록된 명령도 응답이 없어 끝나지 않았거나 TIMEOUT 이다.
        if (device.lockCommandId() != null) {
            VehicleCommand latest = commandRepository.find(device.lockCommandId()).orElseThrow();
            assertThat(latest.type()).isEqualTo(CommandType.DOOR_LOCK);
            assertThat(latest.status()).isNotEqualTo(CommandStatus.ACKNOWLEDGED);
        }
    }

    private ResultActions getRide(String userId, UUID rideId) throws Exception {
        return mockMvc.perform(get("/api/v1/rides/{id}", rideId).with(asRider(userId)));
    }

    private void subscribe(String userId, String plan, Instant startsAt, Instant endsAt) {
        rideJdbcClient.sql("""
                        INSERT INTO subscriptions (user_id, plan_code, starts_at, ends_at)
                        VALUES (:userId, :plan, :startsAt, :endsAt)""")
                .param("userId", userId).param("plan", plan)
                .param("startsAt", Timestamp.from(startsAt)).param("endsAt", Timestamp.from(endsAt))
                .update();
    }

    private JsonNode awaitPaymentRequest(UUID rideId) throws Exception {
        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.paymentRequests())) {
            List<ConsumerRecord<String, String>> records = probe.pollUntil(
                    received -> received.stream().anyMatch(r -> rideId.toString().equals(r.key())), Duration.ofSeconds(20));
            List<ConsumerRecord<String, String>> mine = records.stream()
                    .filter(r -> rideId.toString().equals(r.key())).toList();
            assertThat(mine).as("결제 요청").hasSize(1);
            return objectMapper.readTree(mine.get(0).value());
        }
    }
}
