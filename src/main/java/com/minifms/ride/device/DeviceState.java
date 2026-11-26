package com.minifms.ride.device;

/**
 * 기기 상태 머신.
 * <pre>
 *   AVAILABLE ──대여──▶ IN_USE ──반납(구역·요금 확정 + 결제 요청 적재)──▶ LOCKED ──잠금 명령 ACK──▶ AVAILABLE
 *                                                                     │
 *                                                                     └─ 잠금 거절·무응답: LOCKED 유지, 잠금 명령 재시도
 * </pre>
 * 전이는 DB 조건부 갱신(현재 상태가 기대한 값일 때만)으로만 한다({@link DeviceRepository}).
 * LOCKED 는 반납이 끝났지만 원격 잠금이 확인되지 않은 상태라 대여할 수 없다.
 */
public enum DeviceState {
    AVAILABLE,
    IN_USE,
    LOCKED
}
