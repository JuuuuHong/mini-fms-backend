package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Redis 아웃박스 → Kafka 릴레이 설정.
 *
 * @param relayInterval 릴레이 주기.
 * @param batchSize     한 번에 꺼내 발행하는 최대 건수.
 * @param lockTtl       릴레이 잠금 유효 기간. 여러 인스턴스 중 잠금을 가진 하나만 릴레이한다.
 * @param sendTimeout   한 묶음의 브로커 확인을 기다리는 최대 시간.
 */
@ConfigurationProperties(prefix = "fms.outbox")
public record OutboxProperties(
        @DefaultValue("200ms") Duration relayInterval,
        @DefaultValue("200") int batchSize,
        @DefaultValue("10s") Duration lockTtl,
        @DefaultValue("5s") Duration sendTimeout) {
}
