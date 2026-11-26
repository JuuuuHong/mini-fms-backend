package com.minifms.ride.fare;

/**
 * 반납 때 확정한 요금(원).
 *
 * <pre>
 * total = unlockFee + ridingFee + zoneAdjustment - subscriptionDiscount   (항상 0 이상)
 * </pre>
 *
 * @param ridingMinutes        주행 시간(분, 시작한 분은 올림)
 * @param ridingFee            ridingMinutes × perMinuteFee
 * @param zoneAdjustment       구역 벌금(+) 또는 할인(−). 할인은 남은 금액을 넘지 않게 깎은 값이다.
 * @param subscriptionDiscount 구독권 할인(양수, 합계에서 뺀다)
 */
public record Fare(
        long unlockFee,
        long perMinuteFee,
        long ridingMinutes,
        long ridingFee,
        long zoneAdjustment,
        String subscriptionPlan,
        long subscriptionDiscount,
        long total) {

    public Fare {
        if (total != unlockFee + ridingFee + zoneAdjustment - subscriptionDiscount) {
            throw new IllegalArgumentException("요금 항목의 합이 합계와 다릅니다");
        }
        if (total < 0) {
            throw new IllegalArgumentException("합계는 0 이상이어야 합니다: " + total);
        }
    }
}
