package com.minifms.geofence;

import java.time.Instant;

/**
 * vehicle-geofence-alerts 토픽 메시지.
 *
 * @param alertId        알림 고유 ID. 아웃박스 릴레이는 at-least-once 라서, 소비자는 이 값으로 중복을 거른다.
 * @param distanceMeters 판정 시점의 지오펜스 중심까지 거리
 * @param eventTime      판정에 쓴 텔레메트리의 측정 시각
 * @param detectedAt     서버가 전이를 감지한 시각
 */
public record GeofenceAlert(
        String alertId,
        String vehicleId,
        String geofenceId,
        String geofenceName,
        GeofenceTransition transition,
        double latitude,
        double longitude,
        long distanceMeters,
        Instant eventTime,
        Instant detectedAt) {
}
