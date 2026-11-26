package com.minifms.ride.payment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.minifms.common.config.properties.PaymentOutboxProperties;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 결제 요청 릴레이 설정이 결제 요청을 멈추거나 중복시키는 값이면 기동하지 않는다. */
class PaymentOutboxSettingsTest {

    @Test
    void 확인_대기는_프로듀서_전송_마감보다_길어야_한다() {
        Map<String, Object> producer = Map.of("delivery.timeout.ms", "30000");
        assertThatThrownBy(() -> PaymentOutboxRelay.requireSendTimeoutCoversDelivery(Duration.ofSeconds(5), producer))
                .as("5초에 포기하면 프로듀서가 30초까지 재전송하는 행을 다음 주기가 또 보낸다")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("delivery.timeout.ms");
        assertThatThrownBy(() -> PaymentOutboxRelay.requireSendTimeoutCoversDelivery(Duration.ofSeconds(30), producer))
                .isInstanceOf(IllegalStateException.class);
        assertThatCode(() -> PaymentOutboxRelay.requireSendTimeoutCoversDelivery(Duration.ofSeconds(35), producer))
                .doesNotThrowAnyException();

        // 설정하지 않았으면 Kafka 기본값(120초)과 비교한다.
        assertThatThrownBy(() -> PaymentOutboxRelay.requireSendTimeoutCoversDelivery(Duration.ofSeconds(35), Map.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("120000ms");
        assertThatCode(() -> PaymentOutboxRelay.requireSendTimeoutCoversDelivery(Duration.ofSeconds(125), Map.of(
                "delivery.timeout.ms", 120_000))).doesNotThrowAnyException();
    }

    @Test
    void 묶음_크기와_주기가_0_이하면_거부한다() {
        assertThatThrownBy(() -> properties(0, Duration.ofMillis(200)))
                .as("LIMIT 0 이면 결제 요청이 조용히 영영 나가지 않는다")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("batch-size");
        assertThatThrownBy(() -> properties(-1, Duration.ofMillis(200))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties(100, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("relay-interval");
        assertThatThrownBy(() -> new PaymentOutboxProperties(Duration.ofMillis(200), 100, Duration.ofSeconds(-1),
                Duration.ofDays(7), Duration.ofHours(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatCode(() -> properties(1, Duration.ofMillis(1))).doesNotThrowAnyException();
    }

    private static PaymentOutboxProperties properties(int batchSize, Duration relayInterval) {
        return new PaymentOutboxProperties(relayInterval, batchSize, Duration.ofSeconds(35), Duration.ofDays(7), Duration.ofHours(1));
    }
}
