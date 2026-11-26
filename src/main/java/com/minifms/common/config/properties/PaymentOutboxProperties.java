package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 결제 요청 아웃박스(PostgreSQL) → Kafka 릴레이 설정. 잘못된 값은 기동할 때 거부한다
 * (예: batch-size 0 이면 LIMIT 0 이라 결제 요청이 아무 오류 없이 영영 나가지 않는다).
 *
 * @param relayInterval 릴레이 주기.
 * @param batchSize     한 번에 잠가 발행하는 최대 행 수.
 * @param sendTimeout   한 묶음의 브로커 확인을 기다리는 최대 시간. 그동안 행 잠금(FOR UPDATE)을 쥐고 있다.
 *                      프로듀서의 delivery.timeout.ms 보다 길어야 한다(PaymentOutboxRelay 가 기동할 때 확인한다).
 * @param retention     발행이 확인된 행을 지우기까지의 보존 기간.
 * @param purgeInterval 오래된 행을 지우는 주기.
 */
@ConfigurationProperties(prefix = "fms.payment-outbox")
public record PaymentOutboxProperties(
        @DefaultValue("200ms") Duration relayInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("35s") Duration sendTimeout,
        @DefaultValue("7d") Duration retention,
        @DefaultValue("1h") Duration purgeInterval) {

    public PaymentOutboxProperties {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("fms.payment-outbox.batch-size 는 1 이상이어야 합니다: " + batchSize);
        }
        requirePositive("relay-interval", relayInterval);
        requirePositive("send-timeout", sendTimeout);
        requirePositive("retention", retention);
        requirePositive("purge-interval", purgeInterval);
    }

    private static void requirePositive(String name, Duration value) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException("fms.payment-outbox." + name + " 는 0 보다 커야 합니다: " + value);
        }
    }
}
