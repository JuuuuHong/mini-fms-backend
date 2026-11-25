package com.minifms.incident;

import java.time.Instant;

/**
 * c-its-incident-results 토픽 메시지(키 = incidentId). 돌발상황 한 건을 한 번 처리한 결과다.
 * 같은 incidentId 를 다시 처리하면(보고 측 재시도) version 이 커진 새 결과가 나온다.
 *
 * @param version        incidentId 안에서 단조 증가. 이력 색인은 이 값이 더 큰 결과만 반영한다(늦게 도착한 이전 결과 무시).
 * @param targetCount    경고 대상 차량 수(반경 안 주행 차량)
 * @param confirmedCount Kafka 브로커가 수신을 확인한 경고 수
 * @param failedCount    확인되지 않은 경고 수(발행 실패·확인 시간 초과)
 */
public record IncidentProcessingResult(
        String incidentId,
        long version,
        IncidentType type,
        String sourceId,
        double latitude,
        double longitude,
        Instant occurredAt,
        String description,
        IncidentDispatchStatus status,
        int targetCount,
        int confirmedCount,
        int failedCount,
        Instant processedAt) {
}
