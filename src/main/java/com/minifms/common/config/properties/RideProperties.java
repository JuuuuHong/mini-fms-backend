package com.minifms.common.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * 라이드 대여·반납·정산 설정. DB 연결(fms.ride.datasource)은 DataSourceConfig 가 따로 받는다.
 *
 * @param lockWait       같은 기기에 대한 대여·반납 분산 락을 기다리는 최대 시간. 넘으면 409.
 * @param maxLocationAge 반납 판정에 쓰는 단말 위치(측정 시각)의 최대 나이. 더 오래되면 반납을 받지 않는다.
 */
@Validated
@ConfigurationProperties(prefix = "fms.ride")
public record RideProperties(
        @DefaultValue("3s") Duration lockWait,
        @DefaultValue("30s") Duration maxLocationAge,
        @Valid @DefaultValue Pricing pricing,
        @Valid @DefaultValue LockReconcile lockReconcile) {

    /**
     * 요금표(원). 대여할 때 라이드에 스냅샷으로 남기므로, 바꿔도 이용 중인 라이드의 잠금해제료·분당 요금은 그대로다.
     * 구역 할인·벌금은 반납 시점의 값을 쓴다.
     */
    public record Pricing(
            @DefaultValue("1000") @PositiveOrZero long unlockFee,
            @DefaultValue("150") @PositiveOrZero long perMinuteFee,
            @DefaultValue("500") @PositiveOrZero long discountZoneAmount,
            @DefaultValue("10000") @PositiveOrZero long penaltyZoneAmount,
            @DefaultValue List<@Valid SubscriptionPlan> subscriptionPlans) {

        public Optional<SubscriptionPlan> plan(String code) {
            return subscriptionPlans.stream().filter(plan -> plan.code().equals(code)).findFirst();
        }
    }

    /**
     * 구독권 할인 = (unlockFeeWaived 면 잠금해제료) + 주행요금 × ridingDiscountPercent / 100(원 미만 버림).
     */
    public record SubscriptionPlan(
            @NotBlank String code,
            @DefaultValue("false") boolean unlockFeeWaived,
            @DefaultValue("0") @Min(0) @Max(100) int ridingDiscountPercent) {
    }

    /**
     * 반납 뒤 잠금 명령 발송·확인(DeviceLockReconciler).
     *
     * @param interval             조정 주기.
     * @param pendingCheckInterval 잠금 명령이 진행 중이거나 다른 명령이 끝나기를 기다릴 때 다시 볼 간격.
     * @param retryBase            잠금 명령이 실패(거절·무응답)했을 때 첫 재시도 간격. 실패할 때마다 두 배.
     * @param retryMax             재시도 간격 상한.
     * @param batchSize            한 번에 처리하는 기기 수.
     */
    public record LockReconcile(
            @DefaultValue("500ms") Duration interval,
            @DefaultValue("1s") Duration pendingCheckInterval,
            @DefaultValue("5s") Duration retryBase,
            @DefaultValue("5m") Duration retryMax,
            @DefaultValue("100") @Positive int batchSize) {

        /** attempts 번째 실패 뒤의 대기 시간(retryBase × 2^(attempts-1), 상한 retryMax). */
        public Duration backoff(int attempts) {
            int exponent = Math.min(Math.max(attempts, 1) - 1, 20);
            Duration delay = retryBase.multipliedBy(1L << exponent);
            return delay.compareTo(retryMax) > 0 ? retryMax : delay;
        }
    }
}
