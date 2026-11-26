package com.minifms.ride.fare;

import com.minifms.common.config.properties.RideProperties;
import com.minifms.common.config.properties.RideProperties.Pricing;
import com.minifms.common.config.properties.RideProperties.SubscriptionPlan;
import com.minifms.ride.zone.ZoneType;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 요금 계산(부수 효과 없음).
 *
 * <pre>
 * 이용요금   = 잠금해제료 + 주행시간(분, 올림) × 분당요금
 * 구독권 할인 = (잠금해제료 면제면 잠금해제료) + 주행요금 × 할인율 / 100   (원 미만 버림, 이용요금에만 적용)
 * 구역 조정   = 벌금 구역 +10,000 | 주차 권장 구역 −500(남은 금액까지만) | 그 밖 0
 * 합계       = 잠금해제료 + 주행요금 + 구역 조정 − 구독권 할인
 * </pre>
 * 구독권은 벌금을 깎지 않는다(주차 질서 위반에 대한 비용이므로). 주차 권장 할인은 남은 금액보다 크면 남은 금액까지만
 * 적용해 합계가 음수가 되지 않는다(환불을 만들지 않는다).
 */
@Component
public class FareCalculator {

    private static final long NANOS_PER_MINUTE = 60_000_000_000L;

    private final Pricing pricing;

    @Autowired
    public FareCalculator(RideProperties properties) {
        this(properties.pricing());
    }

    public FareCalculator(Pricing pricing) {
        this.pricing = pricing;
    }

    /** 요금표가 이 구독권을 아는가(판매를 멈춘 구독권이면 false). */
    public boolean offersPlan(String planCode) {
        return pricing.plan(planCode).isPresent();
    }

    /**
     * 지금 요금표로 대여 시점 스냅샷을 만든다. 구독권은 이용자가 대여할 때 지정한 것 하나만 쓴다(자동으로 고르지 않는다).
     *
     * @param planCode 이용자가 지정한 구독권 코드. 없으면 null(정가).
     * @throws IllegalArgumentException 요금표에 없는 구독권({@link #offersPlan} 으로 먼저 확인한다)
     */
    public Tariff tariffFor(String planCode) {
        if (planCode == null) {
            return new Tariff(pricing.unlockFee(), pricing.perMinuteFee(), null, false, 0);
        }
        SubscriptionPlan plan = pricing.plan(planCode)
                .orElseThrow(() -> new IllegalArgumentException("요금표에 없는 구독권입니다: " + planCode));
        return new Tariff(pricing.unlockFee(), pricing.perMinuteFee(), plan.code(), plan.unlockFeeWaived(),
                plan.ridingDiscountPercent());
    }

    /**
     * @param ridingTime 대여~반납 시간. 시작한 분은 1분으로 센다(0초면 0분).
     * @param zoneType   반납 구역. 반납할 수 없는 구역(NO_RETURN_ZONE)은 넘기지 않는다.
     */
    public Fare calculate(Tariff tariff, Duration ridingTime, ZoneType zoneType) {
        if (zoneType == null || !zoneType.returnable()) {
            throw new IllegalArgumentException("반납할 수 없는 구역의 요금은 계산하지 않습니다: " + zoneType);
        }
        // 밀리초로 바꾸면 그 아래가 잘려 60.0004초가 1분이 된다. 나노초로 올림한다(약 292년까지 넘치지 않음).
        long minutes = Math.ceilDiv(Math.max(0L, ridingTime.toNanos()), NANOS_PER_MINUTE);
        long ridingFee = Math.multiplyExact(minutes, tariff.perMinuteFee());

        long subscriptionDiscount = (tariff.unlockFeeWaived() ? tariff.unlockFee() : 0)
                + ridingFee * tariff.ridingDiscountPercent() / 100;
        long payable = tariff.unlockFee() + ridingFee - subscriptionDiscount;

        long zoneAdjustment = switch (zoneType) {
            case PENALTY_ZONE -> pricing.penaltyZoneAmount();
            case DISCOUNT_ZONE -> -Math.min(pricing.discountZoneAmount(), payable);
            case SERVICE_ZONE, NO_RETURN_ZONE -> 0;
        };
        return new Fare(
                tariff.unlockFee(),
                tariff.perMinuteFee(),
                minutes,
                ridingFee,
                zoneAdjustment,
                tariff.subscriptionPlan(),
                subscriptionDiscount,
                payable + zoneAdjustment);
    }
}
