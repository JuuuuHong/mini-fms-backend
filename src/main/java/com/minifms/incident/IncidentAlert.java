package com.minifms.incident;

import java.time.Instant;

/**
 * c-its-incident-alerts 토픽 메시지. 차량 한 대에 한 건씩, 키 = vehicleId 로 발행한다
 * (차량별 V2X 푸시 게이트웨이가 자기 파티션만 읽으면 되도록).
 *
 * @param distanceMeters 경고 시점 차량과 돌발 지점 사이 거리
 * @param expiresAt      이 시각이 지나면 단말은 경고를 표시하지 않는다
 */
public record IncidentAlert(
        String alertId,
        String incidentId,
        String vehicleId,
        IncidentType type,
        double incidentLatitude,
        double incidentLongitude,
        long distanceMeters,
        String sourceId,
        String message,
        Instant occurredAt,
        Instant issuedAt,
        Instant expiresAt) {
}
