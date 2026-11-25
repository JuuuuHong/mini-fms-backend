package com.minifms.geofence;

import com.minifms.common.redis.RedisKeys;
import java.util.Set;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

/**
 * 지오펜스별 "안에 있는 차량" 집합 조회. 집합을 바꾸는 쓰기는 위치 갱신 Lua(location_update.lua)가 한다.
 */
@Repository
public class GeofenceStateRepository {

    private final StringRedisTemplate redis;

    public GeofenceStateRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Set<String> vehiclesInside(String geofenceId) {
        Set<String> members = redis.opsForSet().members(RedisKeys.geofenceInside(geofenceId));
        return members == null ? Set.of() : members;
    }
}
