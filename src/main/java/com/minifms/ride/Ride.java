package com.minifms.ride;

import com.minifms.ride.fare.Fare;
import com.minifms.ride.fare.Tariff;
import java.time.Instant;
import java.util.UUID;

/**
 * @param tariff     대여 시점 요금표·구독권 스냅샷
 * @param returnInfo 반납 판정(완료 전에는 null)
 * @param fare       확정 요금(완료 전에는 null)
 */
public record Ride(
        UUID rideId,
        String userId,
        String deviceId,
        RideStatus status,
        Instant startedAt,
        Tariff tariff,
        RideReturn returnInfo,
        Fare fare) {

    public static Ride start(UUID rideId, String userId, String deviceId, Instant startedAt, Tariff tariff) {
        return new Ride(rideId, userId, deviceId, RideStatus.IN_PROGRESS, startedAt, tariff, null, null);
    }
}
