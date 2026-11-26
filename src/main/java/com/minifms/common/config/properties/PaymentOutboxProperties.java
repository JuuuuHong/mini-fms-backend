package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 결제 요청 아웃박스(PostgreSQL) → Kafka 릴레이 설정.
 *
 * @param relayInterval 릴레이 주기.
 * @param batchSize     한 번에 잠가 발행하는 최대 행 수.
 * @param sendTimeout   한 묶음의 브로커 확인을 기다리는 최대 시간. 그동안 행 잠금(FOR UPDATE)을 쥐고 있다.
 * @param retention     발행이 확인된 행을 지우기까지의 보존 기간.
 * @param purgeInterval 오래된 행을 지우는 주기.
 */
@ConfigurationProperties(prefix = "fms.payment-outbox")
public record PaymentOutboxProperties(
        @DefaultValue("200ms") Duration relayInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("5s") Duration sendTimeout,
        @DefaultValue("7d") Duration retention,
        @DefaultValue("1h") Duration purgeInterval) {
}
