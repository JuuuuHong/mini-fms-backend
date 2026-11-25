package com.minifms.incident.history;

import com.minifms.incident.IncidentDispatchStatus;
import com.minifms.incident.IncidentType;
import java.time.Instant;

/**
 * 관제 이력 API 응답 한 건.
 *
 * @param targetCount    경고 대상 차량 수
 * @param confirmedCount Kafka 브로커가 수신을 확인한 경고 수(차량 단말 수신 여부는 연동 범위 밖)
 * @param version        같은 incidentId 를 여러 번 처리했을 때 가장 최근 처리의 버전
 */
public record IncidentHistoryView(
        String incidentId,
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
        long version,
        Instant processedAt) {

    static IncidentHistoryView from(IncidentHistoryDocument document) {
        return new IncidentHistoryView(
                document.incidentId(),
                document.type(),
                document.sourceId(),
                document.location().lat(),
                document.location().lon(),
                document.occurredAt(),
                document.description(),
                document.status(),
                document.targetCount(),
                document.confirmedCount(),
                document.failedCount(),
                document.version(),
                document.processedAt());
    }
}
