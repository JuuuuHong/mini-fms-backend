package com.minifms.vehicle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.minifms.common.redis.RedisKeys;
import com.minifms.security.Scopes;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.TestEvents;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.vehicle.location.NearbyVehicle;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Redis GEO 반경 조회·순서 보장·활성 판정과 반경 조회 API. */
class VehicleLocationIT extends IntegrationTestSupport {

    // 다른 테스트와 겹치지 않는 기준점(대구 부근)
    static final double LAT = 35.8714;
    static final double LNG = 128.6014;

    @Autowired
    VehicleLocationRepository repository;
    @Autowired
    StringRedisTemplate redis;

    String prefix;
    String near;
    String mid;
    String far;
    String stale;

    @BeforeEach
    void seed() {
        prefix = TestEvents.uniqueId("L");
        near = prefix + "-near";
        mid = prefix + "-mid";
        far = prefix + "-far";
        stale = prefix + "-stale";
        long now = System.currentTimeMillis();
        repository.updateIfNewer(TestEvents.northOf(near, LAT, LNG, 500, now), now);
        repository.updateIfNewer(TestEvents.northOf(mid, LAT, LNG, 2_000, now), now);
        repository.updateIfNewer(TestEvents.northOf(far, LAT, LNG, 10_000, now), now);
        // 5분 전이 마지막 수신 → 비활성
        long fiveMinutesAgo = now - Duration.ofMinutes(5).toMillis();
        repository.updateIfNewer(TestEvents.northOf(stale, LAT, LNG, 300, fiveMinutesAgo), now);
    }

    @Test
    void 반경_안의_활성_차량만_가까운_순으로_돌려준다() {
        long activeSince = System.currentTimeMillis() - Duration.ofSeconds(60).toMillis();

        List<NearbyVehicle> within1km = ours(repository.searchNearby(LAT, LNG, 1.0, activeSince, 100));
        assertThat(within1km).extracting(NearbyVehicle::vehicleId).containsExactly(near);

        List<NearbyVehicle> within3km = ours(repository.searchNearby(LAT, LNG, 3.0, activeSince, 100));
        assertThat(within3km).extracting(NearbyVehicle::vehicleId).containsExactly(near, mid);
        assertThat(within3km.get(0).distanceKm()).isCloseTo(0.5, within(0.01));
        assertThat(within3km.get(1).distanceKm()).isCloseTo(2.0, within(0.01));
        assertThat(within3km.get(0).engineStatus()).isEqualTo(EngineStatus.ON);
    }

    @Test
    void 늦게_도착한_과거_좌표는_최신_위치를_덮어쓰지_않는다() {
        String vehicleId = prefix + "-order";
        assertThat(repository.updateIfNewer(TestEvents.northOf(vehicleId, LAT, LNG, 100, 2_000_000L), 1L)).isTrue();
        assertThat(repository.updateIfNewer(TestEvents.northOf(vehicleId, LAT, LNG, 900, 1_000_000L), 1L)).isFalse();
        assertThat(repository.updateIfNewer(TestEvents.northOf(vehicleId, LAT, LNG, 900, 2_000_000L), 1L)).isFalse();

        assertThat(repository.findLatest(vehicleId)).hasValueSatisfying(latest -> {
            assertThat(latest.timestamp()).isEqualTo(Instant.ofEpochMilli(2_000_000L));
            assertThat(latest.latitude()).isCloseTo(TestEvents.northOf(vehicleId, LAT, LNG, 100, 0).latitude(), within(1e-9));
        });
    }

    @Test
    void 오래_수신이_없는_차량은_GEO_인덱스에서_정리된다() {
        repository.evictStale(Instant.now().minus(Duration.ofMinutes(1)), 1_000);

        assertThat(redis.opsForGeo().position(RedisKeys.VEHICLE_GEO, stale)).containsOnlyNulls();
        assertThat(redis.opsForGeo().position(RedisKeys.VEHICLE_GEO, near)).doesNotContainNull();
    }

    @Test
    void 반경_조회_API() throws Exception {
        mockMvc.perform(get("/api/v1/vehicles/nearby")
                        .param("lat", String.valueOf(LAT)).param("lng", String.valueOf(LNG)).param("radiusKm", "1.0")
                        .with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.radiusKm").value(1.0))
                .andExpect(jsonPath("$.vehicles[*].vehicleId", hasItem(near)))
                .andExpect(jsonPath("$.vehicles[*].vehicleId", not(hasItem(mid))))
                .andExpect(jsonPath("$.vehicles[*].vehicleId", not(hasItem(stale))));
    }

    @Test
    void 반경_조회_API_파라미터_검증() throws Exception {
        mockMvc.perform(get("/api/v1/vehicles/nearby")
                        .param("lat", "95").param("lng", "127").param("radiusKm", "1")
                        .with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/vehicles/nearby")
                        .param("lat", "37").param("lng", "127").param("radiusKm", "0")
                        .with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void 차량_상세_API() throws Exception {
        mockMvc.perform(get("/api/v1/vehicles/{id}", near).with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.latest.vehicleId").value(near))
                .andExpect(jsonPath("$.active").value(true))
                .andExpect(jsonPath("$.controlState.door").value("UNLOCKED"));
        mockMvc.perform(get("/api/v1/vehicles/{id}", prefix + "-none").with(withScopes(Scopes.FLEET_READ)))
                .andExpect(status().isNotFound());
    }

    private List<NearbyVehicle> ours(List<NearbyVehicle> vehicles) {
        return vehicles.stream().filter(v -> v.vehicleId().startsWith(prefix)).toList();
    }
}
