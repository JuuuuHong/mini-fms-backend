package com.minifms.geofence;

import com.minifms.common.redis.RedisKeys;
import java.util.List;
import java.util.Set;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

/** 지오펜스별 "안에 있는 차량" 집합. 전이(ENTER/EXIT) 판정의 이전 상태가 된다. */
@Repository
public class GeofenceStateRepository {

    private static final RedisScript<Long> TRANSITION =
            RedisScript.of(new ClassPathResource("scripts/geofence_transition.lua"), Long.class);

    private final StringRedisTemplate redis;

    public GeofenceStateRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** 상태가 실제로 바뀌었으면 알림을 아웃박스에 넣고 true 를 돌려준다. */
    public boolean transition(String geofenceId, String vehicleId, GeofenceTransition transition, String outboxEntry) {
        Long changed = redis.execute(TRANSITION,
                List.of(RedisKeys.geofenceInside(geofenceId), RedisKeys.OUTBOX),
                vehicleId, transition.name(), outboxEntry);
        return changed != null && changed == 1L;
    }

    public Set<String> vehiclesInside(String geofenceId) {
        Set<String> members = redis.opsForSet().members(RedisKeys.geofenceInside(geofenceId));
        return members == null ? Set.of() : members;
    }
}
