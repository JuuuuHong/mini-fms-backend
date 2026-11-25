package com.minifms.common.idempotency;

import com.minifms.common.config.properties.IdempotencyProperties;
import com.minifms.common.redis.RedisKeys;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis 기반 윈도우 멱등 처리기.
 *
 * <p>키 하나는 세 상태를 오간다.
 * <pre>
 *   (없음) --claim--> 토큰(선점, TTL=lease) --complete--> DONE(TTL=window)
 *                          |
 *                          +--release / lease 만료--> (없음)
 * </pre>
 * <ul>
 *   <li>claim 은 SET NX 로 원자적으로 선점한다. 배치 전체를 Lua 한 번(왕복 1회)으로 처리한다.</li>
 *   <li>"처리 전에 DONE 으로 표시"하는 단순 방식은 처리 실패 시 재전달 메시지를 중복으로 오인해 잃는다.
 *       그래서 선점(토큰)과 완료(DONE)를 나누고, 실패하면 release 로 선점을 푼다.</li>
 *   <li>complete·release 는 자기 토큰으로 선점한 키에만 적용된다. 리스가 만료돼 다른 시도가 다시 선점한 키를
 *       잘못 지우거나 확정하지 않는다.</li>
 * </ul>
 * namespace 는 소비처(sink)별로 나눈다. "처리 완료"는 그 소비처가 반영했다는 뜻이라 소비처마다 따로 기록해야 한다.
 */
@Component
public class IdempotencyGuard {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> CLAIM =
            RedisScript.of(new ClassPathResource("scripts/idempotency_claim.lua"), List.class);
    private static final RedisScript<Long> COMPLETE =
            RedisScript.of(new ClassPathResource("scripts/idempotency_complete.lua"), Long.class);
    private static final RedisScript<Long> RELEASE =
            RedisScript.of(new ClassPathResource("scripts/idempotency_release.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final IdempotencyProperties properties;

    public IdempotencyGuard(StringRedisTemplate redis, IdempotencyProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public ClaimResult claim(String namespace, List<String> businessKeys) {
        String token = "P:" + UUID.randomUUID();
        if (businessKeys.isEmpty()) {
            return new ClaimResult(namespace, token, List.of(), List.of());
        }
        List<?> raw = redis.execute(CLAIM, redisKeys(namespace, businessKeys),
                token, String.valueOf(properties.lease().toMillis()));
        if (raw == null || raw.size() != businessKeys.size()) {
            throw new IllegalStateException("멱등키 선점 결과가 올바르지 않습니다: " + raw);
        }
        List<ClaimStatus> statuses = raw.stream().map(o -> ClaimStatus.valueOf(o.toString())).toList();
        return new ClaimResult(namespace, token, List.copyOf(businessKeys), statuses);
    }

    /** 처리를 마친 키를 DONE 으로 확정한다. 확정한 키 수를 돌려준다. */
    public int complete(ClaimResult claim, Collection<String> businessKeys) {
        if (businessKeys.isEmpty()) {
            return 0;
        }
        Long done = redis.execute(COMPLETE, redisKeys(claim.namespace(), businessKeys),
                claim.token(), String.valueOf(properties.window().toMillis()));
        return done == null ? 0 : done.intValue();
    }

    /** 처리하지 못한 키의 선점을 풀어 재전달 때 다시 처리되게 한다. 푼 키 수를 돌려준다. */
    public int release(ClaimResult claim, Collection<String> businessKeys) {
        if (businessKeys.isEmpty()) {
            return 0;
        }
        Long released = redis.execute(RELEASE, redisKeys(claim.namespace(), businessKeys), claim.token());
        return released == null ? 0 : released.intValue();
    }

    private static List<String> redisKeys(String namespace, Collection<String> businessKeys) {
        return businessKeys.stream().map(key -> RedisKeys.idempotency(namespace, key)).toList();
    }
}
