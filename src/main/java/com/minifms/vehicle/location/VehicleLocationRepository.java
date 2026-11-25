package com.minifms.vehicle.location;

import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.redis.RedisKeys;
import com.minifms.telematics.domain.TelematicsEvent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Repository;

/**
 * 실시간 위치 저장소(Redis).
 * <ul>
 *   <li>fms:vehicles:geo — GEO 인덱스. 반경 조회(GEOSEARCH)에 쓴다.</li>
 *   <li>fms:vehicle:{id}:latest — 최신 텔레메트리 해시(속도·배터리·엔진 상태).</li>
 *   <li>fms:vehicles:last-seen — 마지막 수신 시각. 활성 판정·오래된 차량 정리에 쓴다.</li>
 * </ul>
 * 쓰기는 opsForGeo().add 대신 Lua 로 한다. "저장된 것보다 최신일 때만 GEOADD" 를 원자적으로 해야
 * 지연 도착한 과거 좌표가 최신 위치를 덮어쓰지 않기 때문이다.
 */
@Repository
public class VehicleLocationRepository {

    private static final RedisScript<Long> UPDATE_IF_NEWER =
            RedisScript.of(new ClassPathResource("scripts/location_update.lua"), Long.class);
    private static final RedisScript<Long> EVICT_STALE =
            RedisScript.of(new ClassPathResource("scripts/evict_stale.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final LocationProperties properties;

    public VehicleLocationRepository(StringRedisTemplate redis, LocationProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /** 더 최신 이벤트일 때만 위치를 반영한다. 반영했으면 true. */
    public boolean updateIfNewer(TelematicsEvent event, long ingestedAtMillis) {
        Long applied = redis.execute(UPDATE_IF_NEWER,
                List.of(RedisKeys.VEHICLE_GEO, RedisKeys.vehicleLatest(event.vehicleId()), RedisKeys.VEHICLE_LAST_SEEN),
                event.vehicleId(),
                Double.toString(event.longitude()),
                Double.toString(event.latitude()),
                Long.toString(event.timestamp()),
                Double.toString(event.speed()),
                Double.toString(event.batterySoc()),
                event.engineStatus().name(),
                Long.toString(ingestedAtMillis),
                Long.toString(properties.latestTtl().toSeconds()));
        return applied != null && applied == 1L;
    }

    /**
     * 중심 좌표 반경 안의 활성 차량을 가까운 순으로 돌려준다.
     *
     * @param activeSinceMillis 마지막 수신이 이 시각 이후인 차량만 포함한다.
     */
    public List<NearbyVehicle> searchNearby(double latitude, double longitude, double radiusKm,
                                            long activeSinceMillis, int limit) {
        // GEOSEARCH key FROMLONLAT lng lat BYRADIUS r km WITHDIST ASC
        // COUNT 는 주지 않는다. 비활성 차량을 걸러 낸 뒤에 개수를 자르기 위해서다(반경이 상한이라 결과가 유한하다).
        GeoResults<GeoLocation<String>> results = redis.opsForGeo().search(
                RedisKeys.VEHICLE_GEO,
                GeoReference.fromCoordinate(longitude, latitude),
                new Distance(radiusKm, Metrics.KILOMETERS),
                GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().sortAscending());
        if (results == null || results.getContent().isEmpty()) {
            return List.of();
        }

        List<GeoResult<GeoLocation<String>>> hits = results.getContent();
        Object[] ids = hits.stream().map(hit -> hit.getContent().getName()).toArray();
        List<Double> lastSeen = redis.opsForZSet().score(RedisKeys.VEHICLE_LAST_SEEN, ids);

        List<String> activeIds = new ArrayList<>();
        List<Double> activeDistances = new ArrayList<>();
        for (int i = 0; i < hits.size() && activeIds.size() < limit; i++) {
            Double seen = lastSeen == null ? null : lastSeen.get(i);
            if (seen != null && seen.longValue() >= activeSinceMillis) {
                activeIds.add((String) ids[i]);
                activeDistances.add(hits.get(i).getDistance().getValue());
            }
        }
        if (activeIds.isEmpty()) {
            return List.of();
        }

        // 차량별 최신 상태를 파이프라인 한 번으로 읽는다(N 번 왕복 방지).
        List<Object> hashes = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String id : activeIds) {
                connection.hashCommands().hGetAll(RedisKeys.vehicleLatest(id).getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });

        List<NearbyVehicle> vehicles = new ArrayList<>(activeIds.size());
        for (int i = 0; i < activeIds.size(); i++) {
            @SuppressWarnings("unchecked")
            Map<String, String> hash = (Map<String, String>) hashes.get(i);
            if (hash == null || hash.isEmpty()) {
                continue; // 최신 상태가 만료됐는데 인덱스만 남은 경우
            }
            VehicleSnapshot snapshot = VehicleSnapshot.fromHash(hash);
            vehicles.add(new NearbyVehicle(
                    snapshot.vehicleId(),
                    snapshot.latitude(),
                    snapshot.longitude(),
                    Math.round(activeDistances.get(i) * 1000.0) / 1000.0,
                    snapshot.speed(),
                    snapshot.batterySoc(),
                    snapshot.engineStatus(),
                    snapshot.timestamp()));
        }
        return vehicles;
    }

    public Optional<VehicleSnapshot> findLatest(String vehicleId) {
        Map<String, String> hash = redis.<String, String>opsForHash().entries(RedisKeys.vehicleLatest(vehicleId));
        return hash.isEmpty() ? Optional.empty() : Optional.of(VehicleSnapshot.fromHash(hash));
    }

    public boolean exists(String vehicleId) {
        return Boolean.TRUE.equals(redis.hasKey(RedisKeys.vehicleLatest(vehicleId)));
    }

    /** cutoff 보다 오래 수신이 없는 차량을 GEO 인덱스에서 뺀다. 뺀 수를 돌려준다. */
    public long evictStale(Instant cutoff, int maxCount) {
        Long evicted = redis.execute(EVICT_STALE,
                List.of(RedisKeys.VEHICLE_LAST_SEEN, RedisKeys.VEHICLE_GEO),
                Long.toString(cutoff.toEpochMilli()), Integer.toString(maxCount));
        return evicted == null ? 0 : evicted;
    }
}
