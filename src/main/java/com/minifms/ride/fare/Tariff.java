package com.minifms.ride.fare;

/**
 * 대여 시점의 요금표·구독권 스냅샷. 라이드에 저장해 두고 반납 때 이 값으로 정산한다
 * (이용 중에 요금표가 바뀌거나 구독권 기간이 끝나도 대여할 때 보여 준 조건을 지킨다).
 *
 * @param subscriptionPlan      적용한 구독권 코드(없으면 null)
 * @param unlockFeeWaived       구독권의 잠금해제료 면제 여부
 * @param ridingDiscountPercent 구독권의 주행요금 할인율(0~100)
 */
public record Tariff(
        long unlockFee,
        long perMinuteFee,
        String subscriptionPlan,
        boolean unlockFeeWaived,
        int ridingDiscountPercent) {

    public Tariff {
        if (unlockFee < 0 || perMinuteFee < 0) {
            throw new IllegalArgumentException("요금은 0 이상이어야 합니다");
        }
        if (ridingDiscountPercent < 0 || ridingDiscountPercent > 100) {
            throw new IllegalArgumentException("할인율은 0~100 이어야 합니다: " + ridingDiscountPercent);
        }
    }
}
