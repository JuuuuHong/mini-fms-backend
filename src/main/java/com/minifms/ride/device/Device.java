package com.minifms.ride.device;

import java.time.Instant;
import java.util.UUID;

/**
 * @param currentRideId  IN_USE 일 때 진행 중인 라이드
 * @param lockCommandId  LOCKED 일 때 진행 중이거나 마지막으로 보낸 잠금 명령(실패하면 비우고 재시도)
 * @param lockAttempts   이번 반납 뒤 보낸 잠금 명령 수
 * @param lockRetryAt    LOCKED 일 때 잠금 조정기가 다시 볼 시각
 */
public record Device(
        String deviceId,
        DeviceState state,
        UUID currentRideId,
        String lockCommandId,
        int lockAttempts,
        Instant lockRetryAt,
        Instant stateChangedAt,
        Instant registeredAt) {
}
