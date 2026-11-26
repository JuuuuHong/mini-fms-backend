package com.minifms.ride;

import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** 구독권(읽기 전용). 판매·결제는 이 서비스 범위 밖이다. */
@Repository
public class SubscriptionRepository {

    private final JdbcClient jdbc;

    public SubscriptionRepository(JdbcClient rideJdbcClient) {
        this.jdbc = rideJdbcClient;
    }

    /** 이용자가 at 시점에 유효한([starts_at, ends_at)) 이 구독권을 갖고 있는가. */
    public boolean hasActive(String userId, String planCode, Instant at) {
        return jdbc.sql("""
                        SELECT EXISTS (SELECT 1 FROM subscriptions
                                       WHERE user_id = :userId AND plan_code = :planCode
                                         AND starts_at <= :at AND ends_at > :at)""")
                .param("userId", userId)
                .param("planCode", planCode)
                .param("at", Timestamp.from(at))
                .query(Boolean.class)
                .single();
    }
}
