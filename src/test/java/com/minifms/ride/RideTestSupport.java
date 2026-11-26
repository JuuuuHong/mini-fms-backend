package com.minifms.ride;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.minifms.ride.device.Device;
import com.minifms.ride.device.DeviceRepository;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Duration;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.JwtRequestPostProcessor;
import org.springframework.test.web.servlet.ResultActions;

/**
 * 라이드 통합 테스트 도우미. 좌표는 테스트 프로필의 반납 구역(대전, application-test.yml)과 같다.
 * <pre>
 *   T-SERVICE   (36.340~36.360, 127.370~127.390)
 *   T-DISCOUNT  (36.351~36.352, 127.381~127.382)
 *   T-PENALTY   (36.348~36.349, 127.381~127.382)
 *   T-NO-RETURN (36.350~36.351, 127.378~127.379)
 * </pre>
 */
abstract class RideTestSupport extends IntegrationTestSupport {

    static final double[] SERVICE = {36.3450, 127.3750};
    static final double[] DISCOUNT = {36.3515, 127.3815};
    static final double[] PENALTY = {36.3485, 127.3815};
    static final double[] NO_RETURN = {36.3505, 127.3785};
    static final double[] OUTSIDE = {36.3700, 127.3800};

    @Autowired
    VehicleLocationRepository locationRepository;
    @Autowired
    DeviceRepository deviceRepository;
    @Autowired
    JdbcClient rideJdbcClient;

    /** 기기를 등록한다(관제 API). 위치는 보내지 않는다. */
    String registerDevice(String deviceId) throws Exception {
        mockMvc.perform(put("/api/v1/devices/{id}", deviceId).with(withScopes(Scopes.VEHICLE_CONTROL)))
                .andExpect(status().is2xxSuccessful());
        return deviceId;
    }

    /** 기기를 등록하고 지금 이 좌표에 있다고 보고한 상태로 만든다. */
    String registerDeviceAt(String deviceId, double[] position) throws Exception {
        registerDevice(deviceId);
        moveTo(deviceId, position, System.currentTimeMillis());
        return deviceId;
    }

    /** 단말이 측정 시각 timestamp 에 이 좌표를 보고한 것으로 만든다(더 최신 시각일 때만 반영). */
    void moveTo(String deviceId, double[] position, long timestamp) {
        locationRepository.updateIfNewer(
                new TelematicsEvent(deviceId, position[0], position[1], 0.0, 80.0, EngineStatus.OFF, timestamp),
                System.currentTimeMillis());
    }

    ResultActions rentRequest(String userId, String deviceId) throws Exception {
        return mockMvc.perform(post("/api/v1/rides").with(asRider(userId))
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"deviceId\":\"" + deviceId + "\"}"));
    }

    UUID rent(String userId, String deviceId) throws Exception {
        String body = rentRequest(userId, deviceId).andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(JsonPath.read(body, "$.rideId"));
    }

    ResultActions returnRequest(String userId, UUID rideId) throws Exception {
        return mockMvc.perform(post("/api/v1/rides/{id}/return", rideId).with(asRider(userId)));
    }

    /** 대여 시각을 앞당겨 주행 시간을 만든다(요금 검증용). */
    void backdateStart(UUID rideId, Duration rideLength) {
        rideJdbcClient.sql("UPDATE rides SET started_at = started_at - CAST(:interval AS interval) WHERE ride_id = :id")
                .param("interval", rideLength.toMillis() + " milliseconds")
                .param("id", rideId)
                .update();
    }

    long paymentOutboxRows(UUID rideId) {
        return rideJdbcClient.sql("SELECT count(*) FROM payment_outbox WHERE ride_id = :id")
                .param("id", rideId).query(Long.class).single();
    }

    Device device(String deviceId) {
        return deviceRepository.find(deviceId).orElseThrow();
    }

    static JwtRequestPostProcessor asRider(String userId) {
        return withScopes(Scopes.RIDE_WRITE).jwt(jwt -> jwt.subject(userId));
    }
}
