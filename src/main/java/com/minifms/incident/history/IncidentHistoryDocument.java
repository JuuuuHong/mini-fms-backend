package com.minifms.incident.history;

import com.minifms.incident.IncidentDispatchStatus;
import com.minifms.incident.IncidentProcessingResult;
import com.minifms.incident.IncidentType;
import java.time.Instant;

/**
 * Elasticsearch 문서(색인 c-its-incidents, 문서 ID = incidentId). 문서 버전은 처리 결과의 version 이다(external).
 *
 * @param location geo_point({lat, lon}). 나중에 "이 지점 주변 과거 돌발상황" 같은 공간 검색에 쓴다.
 */
public record IncidentHistoryDocument(
        String incidentId,
        long version,
        IncidentType type,
        String sourceId,
        GeoLocation location,
        Instant occurredAt,
        String description,
        IncidentDispatchStatus status,
        int targetCount,
        int confirmedCount,
        int failedCount,
        Instant processedAt) {

    public static IncidentHistoryDocument from(IncidentProcessingResult result) {
        return new IncidentHistoryDocument(
                result.incidentId(),
                result.version(),
                result.type(),
                result.sourceId(),
                new GeoLocation(result.latitude(), result.longitude()),
                result.occurredAt(),
                result.description(),
                result.status(),
                result.targetCount(),
                result.confirmedCount(),
                result.failedCount(),
                result.processedAt());
    }

    public record GeoLocation(double lat, double lon) {
    }
}
