package com.minifms.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.minifms.common.config.properties.IdempotencyProperties;
import com.minifms.common.idempotency.ClaimResult;
import com.minifms.common.idempotency.ClaimStatus;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.TestEvents;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Redis 멱등 처리기: 선점·중복·처리 중·해제·윈도우 만료. */
class IdempotencyGuardIT extends IntegrationTestSupport {

    @Autowired
    StringRedisTemplate redis;

    IdempotencyGuard guard;
    String namespace;

    @BeforeEach
    void setUp() {
        // 만료 동작을 빨리 보려고 윈도우·리스를 짧게 준 별도 인스턴스를 쓴다.
        guard = new IdempotencyGuard(redis, new IdempotencyProperties(Duration.ofMillis(800), Duration.ofMillis(800)));
        namespace = TestEvents.uniqueId("ns");
    }

    @Test
    void 같은_배치_안의_중복은_한_번만_선점된다() {
        ClaimResult claim = guard.claim(namespace, List.of("V-1:1000", "V-1:2000", "V-1:1000"));

        assertThat(claim.statuses()).containsExactly(ClaimStatus.CLAIMED, ClaimStatus.CLAIMED, ClaimStatus.DUPLICATE);
        assertThat(claim.claimedKeys()).containsExactly("V-1:1000", "V-1:2000");
    }

    @Test
    void 처리_완료된_키는_윈도우_동안_중복이고_윈도우가_지나면_다시_처리된다() {
        ClaimResult first = guard.claim(namespace, List.of("V-1:1000"));
        assertThat(guard.complete(first, first.claimedKeys())).isEqualTo(1);

        assertThat(guard.claim(namespace, List.of("V-1:1000")).status(0)).isEqualTo(ClaimStatus.DUPLICATE);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(guard.claim(namespace, List.of("V-1:1000")).status(0)).isEqualTo(ClaimStatus.CLAIMED));
    }

    @Test
    void 다른_시도가_선점_중이면_IN_FLIGHT_이고_해제하면_다시_선점된다() {
        ClaimResult first = guard.claim(namespace, List.of("V-2:1000"));

        ClaimResult concurrent = guard.claim(namespace, List.of("V-2:1000"));
        assertThat(concurrent.status(0)).isEqualTo(ClaimStatus.IN_FLIGHT);

        // 다른 토큰으로는 풀거나 확정할 수 없다.
        assertThat(guard.release(concurrent, List.of("V-2:1000"))).isZero();
        assertThat(guard.complete(concurrent, List.of("V-2:1000"))).isZero();

        assertThat(guard.release(first, List.of("V-2:1000"))).isEqualTo(1);
        assertThat(guard.claim(namespace, List.of("V-2:1000")).status(0)).isEqualTo(ClaimStatus.CLAIMED);
    }

    @Test
    void 선점한_시도가_죽어도_리스가_지나면_다시_선점된다() {
        guard.claim(namespace, List.of("V-3:1000")); // complete·release 없이 버려진 선점

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(guard.claim(namespace, List.of("V-3:1000")).status(0)).isEqualTo(ClaimStatus.CLAIMED));
    }
}
