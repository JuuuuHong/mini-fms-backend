package com.minifms.ride;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** 구독권(읽기 전용). 판매·결제는 이 서비스 범위 밖이다. */
@Repository
public class SubscriptionRepository {

    private final JdbcClient jdbc;

    public SubscriptionRepository(JdbcClient rideJdbcClient) {
        this.jdbc = rideJdbcClient;
    }

    /** at 시점에 유효한 구독권 코드([starts_at, ends_at)). */
    public List<String> findActivePlanCodes(String userId, Instant at) {
        return jdbc.sql("""
                        SELECT DISTINCT plan_code FROM subscriptions
                        WHERE user_id = :userId AND starts_at <= :at AND ends_at > :at""")
                .param("userId", userId)
                .param("at", Timestamp.from(at))
                .query(String.class)
                .list();
    }
}
