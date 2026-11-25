package com.minifms.vehicle.location;

import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.redis.RedisKeys;
import com.minifms.geofence.GeofenceCandidate;
import com.minifms.telematics.domain.TelematicsEvent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
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
 * 지연 도착한 과거 좌표가 최신 위치를 덮어쓰지 않고, 지오펜스 전이도 같은 연산 안에서 반영해야
 * 위치만 저장된 채 알림이 빠지는 일이 없기 때문이다.
 */
@Repository
public class VehicleLocationRepository {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> UPDATE_IF_NEWER =
            RedisScript.of(new ClassPathResource("scripts/location_update.lua"), List.class);
    private static final RedisScript<Long> EVICT_STALE =
            RedisScript.of(new ClassPathResource("scripts/evict_stale.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final LocationProperties properties;

    public VehicleLocationRepository(StringRedisTemplate redis, LocationProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /** 더 최신 이벤트일 때만 위치를 반영한다(지오펜스 판정 없음). 반영했으면 true. */
    public boolean updateIfNewer(TelematicsEvent event, long ingestedAtMillis) {
        return applyIfNewer(event, ingestedAtMillis, List.of()).applied();
    }

    /**
     * 더 최신 이벤트일 때만 위치를 반영하고, 같은 원자 연산 안에서 지오펜스 전이 후보를 적용한다.
     * 이전 상태와 다른 후보만 실제 전이가 되며 그 알림은 아웃박스에 들어간다.
     *
     * <p>응답을 받기 전에 실패해도(연결 끊김 등) 재전달 때 결과가 같다: 스크립트가 실행됐다면 위치와 지오펜스가
     * 함께 반영돼 있어 다시 실행해도 아무것도 바뀌지 않고, 실행되지 않았다면 이번에 함께 반영된다.
     */
    public LocationUpdate applyIfNewer(TelematicsEvent event, long ingestedAtMillis, List<GeofenceCandidate> candidates) {
        List<String> keys = new ArrayList<>(4 + candidates.size());
        keys.add(RedisKeys.VEHICLE_GEO);
        keys.add(RedisKeys.vehicleLatest(event.vehicleId()));
        keys.add(RedisKeys.VEHICLE_LAST_SEEN);
        keys.add(RedisKeys.OUTBOX);
        List<String> args = new ArrayList<>(9 + candidates.size() * 2);
        args.add(event.vehicleId());
        args.add(Double.toString(event.longitude()));
        args.add(Double.toString(event.latitude()));
        args.add(Long.toString(event.timestamp()));
        args.add(Double.toString(event.speed()));
        args.add(Double.toString(event.batterySoc()));
        args.add(event.engineStatus().name());
        args.add(Long.toString(ingestedAtMillis));
        args.add(Long.toString(properties.latestTtl().toSeconds()));
        for (GeofenceCandidate candidate : candidates) {
            keys.add(RedisKeys.geofenceInside(candidate.geofenceId()));
            args.add(candidate.transition().name());
            args.add(candidate.outboxEntry());
        }

        List<?> result = redis.execute(UPDATE_IF_NEWER, keys, args.toArray());
        if (result == null || result.isEmpty()) {
            throw new IllegalStateException("위치 갱신 결과가 비어 있습니다");
        }
        if (((Number) result.get(0)).longValue() != 1L) {
            return LocationUpdate.IGNORED;
        }
        List<GeofenceCandidate> transitions = new ArrayList<>();
        for (int i = 0; i < candidates.size(); i++) {
            if (((Number) result.get(i + 1)).longValue() == 1L) {
                transitions.add(candidates.get(i));
            }
        }
        return new LocationUpdate(true, transitions);
    }

    /**
     * 중심 좌표 반경 안의 활성 차량을 가까운 순으로 돌려준다.
     *
     * @param activeSinceMillis 마지막 수신이 이 시각 이후인 차량만 포함한다.
     */
    public List<NearbyVehicle> searchNearby(double latitude, double longitude, double radiusKm,
                                            long activeSinceMillis, int limit) {
        return searchNearby(latitude, longitude, radiusKm, activeSinceMillis, limit, vehicle -> true);
    }

    /**
     * 조건(filter)을 만족하는 활성 차량을 가까운 순으로 최대 limit 대 돌려준다. 상한은 조건을 적용한 뒤에 센다.
     */
    public List<NearbyVehicle> searchNearby(double latitude, double longitude, double radiusKm,
                                            long activeSinceMillis, int limit, Predicate<NearbyVehicle> filter) {
        // GEOSEARCH key FROMLONLAT lng lat BYRADIUS r km WITHDIST ASC
        // COUNT 는 주지 않는다. 비활성·조건 불일치 차량을 걸러 낸 뒤에 개수를 자르기 위해서다(반경이 상한이라 결과가 유한하다).
        GeoResults<GeoLocation<String>> results = redis.opsForGeo().search(
                RedisKeys.VEHICLE_GEO,
                GeoReference.fromCoordinate(longitude, latitude),
                new Distance(radiusKm, Metrics.KILOMETERS),
                GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().sortAscending());
        if (results == null || results.getContent().isEmpty() || limit <= 0) {
            return List.of();
        }

        List<GeoResult<GeoLocation<String>>> hits = results.getContent();
        Object[] ids = hits.stream().map(hit -> hit.getContent().getName()).toArray();
        List<Double> lastSeen = redis.opsForZSet().score(RedisKeys.VEHICLE_LAST_SEEN, ids);
        List<String> activeIds = new ArrayList<>();
        List<Double> activeDistances = new ArrayList<>();
        for (int i = 0; i < hits.size(); i++) {
            Double seen = lastSeen == null ? null : lastSeen.get(i);
            if (seen != null && seen.longValue() >= activeSinceMillis) {
                activeIds.add((String) ids[i]);
                activeDistances.add(hits.get(i).getDistance().getValue());
            }
        }

        // 가까운 순으로 limit 대씩 최신 상태를 파이프라인으로 읽어, 조건을 만족하는 차량이 limit 대가 될 때까지 채운다.
        List<NearbyVehicle> vehicles = new ArrayList<>();
        for (int from = 0; from < activeIds.size() && vehicles.size() < limit; from += limit) {
            List<String> chunk = activeIds.subList(from, Math.min(from + limit, activeIds.size()));
            List<Object> hashes = redis.executePipelined((RedisCallback<Object>) connection -> {
                for (String id : chunk) {
                    connection.hashCommands().hGetAll(RedisKeys.vehicleLatest(id).getBytes(StandardCharsets.UTF_8));
                }
                return null;
            });
            for (int i = 0; i < chunk.size() && vehicles.size() < limit; i++) {
                @SuppressWarnings("unchecked")
                Map<String, String> hash = (Map<String, String>) hashes.get(i);
                if (hash == null || hash.isEmpty()) {
                    continue; // 최신 상태가 만료됐는데 인덱스만 남은 경우
                }
                VehicleSnapshot snapshot = VehicleSnapshot.fromHash(hash);
                NearbyVehicle vehicle = new NearbyVehicle(
                        snapshot.vehicleId(),
                        snapshot.latitude(),
                        snapshot.longitude(),
                        Math.round(activeDistances.get(from + i) * 1000.0) / 1000.0,
                        snapshot.speed(),
                        snapshot.batterySoc(),
                        snapshot.engineStatus(),
                        snapshot.timestamp());
                if (filter.test(vehicle)) {
                    vehicles.add(vehicle);
                }
            }
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
