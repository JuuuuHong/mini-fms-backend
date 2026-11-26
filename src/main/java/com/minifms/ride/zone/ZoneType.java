package com.minifms.ride.zone;

/**
 * 반납 구역 정책. 한 좌표가 여러 구역에 속하면 precedence 가 큰(더 제한적인) 구역 하나로 판정한다.
 * 예: 서비스 구역 안의 횡단보도(벌금 구역)는 벌금 구역, 주차 권장 구역과 반납 불가 구역이 겹친 곳은 반납 불가.
 */
public enum ZoneType {
    /** 정상 반납. */
    SERVICE_ZONE(1, true),
    /** 주차 권장 구역 — 할인. */
    DISCOUNT_ZONE(2, true),
    /** 주차 금지·통행 방해 구역 — 반납은 받되 벌금. */
    PENALTY_ZONE(3, true),
    /** 반납 불가 — 반납 요청을 거절하고 라이드를 계속 진행 중으로 둔다. */
    NO_RETURN_ZONE(4, false);

    private final int precedence;
    private final boolean returnable;

    ZoneType(int precedence, boolean returnable) {
        this.precedence = precedence;
        this.returnable = returnable;
    }

    public int precedence() {
        return precedence;
    }

    public boolean returnable() {
        return returnable;
    }
}
