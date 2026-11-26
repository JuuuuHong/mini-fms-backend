package com.minifms.ride;

import com.minifms.ride.zone.ZoneType;
import java.time.Instant;

/**
 * 반납 판정에 쓴 사실.
 *
 * @param latitude           판정에 쓴 단말의 최신 GPS 위도(클라이언트가 보낸 좌표가 아니다)
 * @param locationMeasuredAt 그 위치의 단말 측정 시각
 */
public record RideReturn(
        Instant endedAt,
        double latitude,
        double longitude,
        Instant locationMeasuredAt,
        String zoneId,
        ZoneType zoneType) {
}
