package com.minifms.ride.payment;

import com.minifms.ride.Ride;
import com.minifms.ride.fare.Fare;
import com.minifms.ride.zone.ZoneType;
import java.time.Instant;
import java.util.UUID;

/**
 * 결제 요청(ride-payment-requests, 키 = rideId). 결제 서비스가 소비해 외부 PG 에 청구한다.
 *
 * <p>릴레이는 at-least-once 라 같은 이벤트가 두 번 갈 수 있다. 결제 서비스는 {@code idempotencyKey}(라이드당 하나)로
 * 중복 청구를 막는다(PG 의 멱등키로 그대로 넘길 수 있다).
 */
public record PaymentRequestedEvent(
        UUID eventId,
        String idempotencyKey,
        UUID rideId,
        String userId,
        String deviceId,
        long amount,
        String currency,
        Fare fare,
        String zoneId,
        ZoneType zoneType,
        Instant rideStartedAt,
        Instant rideEndedAt,
        Instant requestedAt) {

    public static PaymentRequestedEvent of(UUID eventId, Ride ride, Instant requestedAt) {
        return new PaymentRequestedEvent(
                eventId,
                "ride-payment:" + ride.rideId(),
                ride.rideId(),
                ride.userId(),
                ride.deviceId(),
                ride.fare().total(),
                "KRW",
                ride.fare(),
                ride.returnInfo().zoneId(),
                ride.returnInfo().zoneType(),
                ride.startedAt(),
                ride.returnInfo().endedAt(),
                requestedAt);
    }
}
