package com.minifms.ride.fare;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.minifms.common.config.properties.RideProperties.LockReconcile;
import com.minifms.common.config.properties.RideProperties.Pricing;
import com.minifms.common.config.properties.RideProperties.SubscriptionPlan;
import com.minifms.ride.zone.ZoneType;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 잠금해제료 1,000원, 분당 150원, 주차 권장 −500원, 벌금 +10,000원. 기대값은 손으로 계산한 값이다. */
class FareCalculatorTest {

    final FareCalculator calculator = new FareCalculator(new Pricing(1_000, 150, 500, 10_000, List.of(
            new SubscriptionPlan("PASS_30", false, 30),
            new SubscriptionPlan("PASS_UNLOCK_FREE", true, 0),
            new SubscriptionPlan("PASS_FREE", true, 100))));

    final Tariff noSubscription = calculator.tariffFor(null);

    @Test
    void 주행_시간은_시작한_분을_올림한다() {
        assertThat(fare(noSubscription, Duration.ZERO, ZoneType.SERVICE_ZONE).ridingMinutes()).isZero();
        assertThat(fare(noSubscription, Duration.ofSeconds(1), ZoneType.SERVICE_ZONE).ridingMinutes()).isEqualTo(1);
        assertThat(fare(noSubscription, Duration.ofSeconds(60), ZoneType.SERVICE_ZONE).ridingMinutes()).isEqualTo(1);
        assertThat(fare(noSubscription, Duration.ofSeconds(61), ZoneType.SERVICE_ZONE).ridingMinutes()).isEqualTo(2);
        assertThat(fare(noSubscription, Duration.ofSeconds(60).plusNanos(400_000), ZoneType.SERVICE_ZONE).ridingMinutes())
                .as("1분을 0.4ms 넘김 — 밀리초 아래를 버리면 1분이 된다").isEqualTo(2);
        assertThat(fare(noSubscription, Duration.ofSeconds(60).plusNanos(1_000), ZoneType.SERVICE_ZONE).ridingMinutes())
                .as("1분을 1µs 넘김(DB 정밀도)").isEqualTo(2);
        assertThat(fare(noSubscription, Duration.ofSeconds(-5), ZoneType.SERVICE_ZONE).ridingMinutes())
                .as("시계가 거꾸로 가도 음수 요금 없음").isZero();
    }

    @Test
    void 정상_구역은_잠금해제료와_주행요금만() {
        Fare fare = fare(noSubscription, Duration.ofMinutes(10).plusSeconds(30), ZoneType.SERVICE_ZONE);
        // 11분 × 150 = 1,650 → 1,000 + 1,650 = 2,650
        assertThat(fare.ridingFee()).isEqualTo(1_650);
        assertThat(fare.zoneAdjustment()).isZero();
        assertThat(fare.subscriptionDiscount()).isZero();
        assertThat(fare.total()).isEqualTo(2_650);
    }

    @Test
    void 벌금_구역은_10000원을_더하고_구독권은_벌금을_깎지_않는다() {
        Tariff pass = calculator.tariffFor("PASS_FREE");
        Fare fare = fare(pass, Duration.ofMinutes(5), ZoneType.PENALTY_ZONE);
        // 이용요금 1,000 + 750 은 전액 면제, 벌금 10,000 은 그대로
        assertThat(fare.subscriptionDiscount()).isEqualTo(1_750);
        assertThat(fare.zoneAdjustment()).isEqualTo(10_000);
        assertThat(fare.total()).isEqualTo(10_000);
    }

    @Test
    void 주차_권장_구역은_500원_할인하되_남은_금액을_넘지_않는다() {
        Fare normal = fare(noSubscription, Duration.ofMinutes(3), ZoneType.DISCOUNT_ZONE);
        assertThat(normal.zoneAdjustment()).isEqualTo(-500);
        assertThat(normal.total()).isEqualTo(1_000 + 450 - 500);

        Fare free = fare(calculator.tariffFor("PASS_FREE"), Duration.ofMinutes(3), ZoneType.DISCOUNT_ZONE);
        assertThat(free.zoneAdjustment()).as("남은 금액 0 원").isZero();
        assertThat(free.total()).isZero();

        Fare almostFree = fare(calculator.tariffFor("PASS_UNLOCK_FREE"), Duration.ofMinutes(2), ZoneType.DISCOUNT_ZONE);
        assertThat(almostFree.zoneAdjustment()).as("남은 300 원까지만").isEqualTo(-300);
        assertThat(almostFree.total()).isZero();
    }

    @Test
    void 구독권_할인은_잠금해제료_면제와_주행요금_할인율이고_원_미만은_버린다() {
        Fare fare = fare(calculator.tariffFor("PASS_30"), Duration.ofMinutes(7), ZoneType.SERVICE_ZONE);
        // 주행 7 × 150 = 1,050 → 30% = 315
        assertThat(fare.subscriptionPlan()).isEqualTo("PASS_30");
        assertThat(fare.subscriptionDiscount()).isEqualTo(315);
        assertThat(fare.total()).isEqualTo(1_000 + 1_050 - 315);

        Fare odd = fare(calculator.tariffFor("PASS_30"), Duration.ofMinutes(1), ZoneType.SERVICE_ZONE);
        assertThat(odd.subscriptionDiscount()).as("150 × 30% = 45").isEqualTo(45);
        Tariff odd7 = new Tariff(1_000, 155, "X", false, 33);
        assertThat(fare(odd7, Duration.ofMinutes(1), ZoneType.SERVICE_ZONE).subscriptionDiscount())
                .as("155 × 33% = 51.15 → 51").isEqualTo(51);
    }

    @Test
    void 구독권은_지정한_것만_쓰고_지정하지_않으면_정가다() {
        Tariff none = calculator.tariffFor(null);
        assertThat(none.subscriptionPlan()).isNull();
        assertThat(none.unlockFeeWaived()).isFalse();
        assertThat(none.ridingDiscountPercent()).isZero();
        assertThat(calculator.tariffFor("PASS_UNLOCK_FREE").subscriptionPlan()).isEqualTo("PASS_UNLOCK_FREE");

        assertThat(calculator.offersPlan("PASS_30")).isTrue();
        assertThat(calculator.offersPlan("DISCONTINUED")).isFalse();
        assertThatThrownBy(() -> calculator.tariffFor("DISCONTINUED")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 설정의_구독권_코드는_대여_요청과_같은_형식이어야_한다() {
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            Validator validator = factory.getValidator();
            assertThat(validator.validate(new SubscriptionPlan("PASS_30", false, 30))).isEmpty();
            // 형식이 다르면 판매 중인 구독권을 대여 때 지정할 수 없다(요청 검증 400).
            assertThat(validator.validate(new SubscriptionPlan("pass-30", false, 30))).isNotEmpty();
            assertThat(validator.validate(new SubscriptionPlan("P".repeat(33), false, 30))).isNotEmpty();
        }
    }

    @Test
    void 반납_불가_구역의_요금은_계산하지_않는다() {
        assertThatThrownBy(() -> fare(noSubscription, Duration.ofMinutes(1), ZoneType.NO_RETURN_ZONE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fare(noSubscription, Duration.ofMinutes(1), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 잠금_재시도_간격은_두_배씩_늘고_상한에서_멈춘다() {
        LockReconcile reconcile = new LockReconcile(Duration.ofMillis(500), Duration.ofSeconds(1),
                Duration.ofSeconds(5), Duration.ofMinutes(1), 100);
        assertThat(reconcile.backoff(1)).isEqualTo(Duration.ofSeconds(5));
        assertThat(reconcile.backoff(2)).isEqualTo(Duration.ofSeconds(10));
        assertThat(reconcile.backoff(4)).isEqualTo(Duration.ofSeconds(40));
        assertThat(reconcile.backoff(5)).isEqualTo(Duration.ofMinutes(1));
        assertThat(reconcile.backoff(1_000)).isEqualTo(Duration.ofMinutes(1));
    }

    private Fare fare(Tariff tariff, Duration duration, ZoneType zone) {
        return calculator.calculate(tariff, duration, zone);
    }
}
