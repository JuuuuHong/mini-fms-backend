package com.minifms.geofence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.GeofenceProperties;
import com.minifms.common.config.properties.GeofenceProperties.Fence;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.geo.GeoDistance;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.outbox.OutboxMessage;
import com.minifms.telematics.domain.TelematicsEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 지오펜스 진입·이탈 감지.
 *
 * <p>판정 규칙(히스테리시스):
 * <pre>
 *   거리 ≤ radius                 → 안(ENTER 후보)
 *   거리 > radius + hysteresis    → 밖(EXIT 후보)
 *   그 사이(완충 구간)            → 상태 유지
 * </pre>
 * 후보가 이전 상태와 다를 때만 알림이 나간다(이전 상태는 Redis 집합, 비교·변경·알림 적재는 Lua 로 원자적).
 * 최신 위치를 실제로 갱신한 이벤트만 판정하므로, 늦게 도착한 과거 좌표가 상태를 되돌리지 않는다.
 *
 * <p>지오펜스 수가 많아지면 모든 펜스와 거리를 재는 대신 공간 색인(펜스 중심 GEO 인덱스로 후보 추리기)이 필요하다.
 */
@Component
public class GeofenceDetector {

    private static final Logger log = LoggerFactory.getLogger(GeofenceDetector.class);

    private final GeofenceProperties properties;
    private final GeofenceStateRepository stateRepository;
    private final TopicProperties topics;
    private final ObjectMapper objectMapper;
    private final FmsMetrics metrics;
    private final Clock clock;

    public GeofenceDetector(GeofenceProperties properties, GeofenceStateRepository stateRepository,
                            TopicProperties topics, ObjectMapper objectMapper, FmsMetrics metrics, Clock clock) {
        this.properties = properties;
        this.stateRepository = stateRepository;
        this.topics = topics;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** 이번 이벤트로 일어난 전이 목록(대부분 비어 있다). */
    public List<GeofenceAlert> evaluate(TelematicsEvent event) {
        List<GeofenceAlert> alerts = new ArrayList<>();
        for (Fence fence : properties.fences()) {
            double distance = GeoDistance.haversineMeters(
                    fence.latitude(), fence.longitude(), event.latitude(), event.longitude());
            GeofenceTransition candidate;
            if (distance <= fence.radiusMeters()) {
                candidate = GeofenceTransition.ENTER;
            } else if (distance > fence.radiusMeters() + fence.hysteresisMeters()) {
                candidate = GeofenceTransition.EXIT;
            } else {
                continue;
            }

            GeofenceAlert alert = new GeofenceAlert(
                    UUID.randomUUID().toString(),
                    event.vehicleId(),
                    fence.id(),
                    fence.name(),
                    candidate,
                    event.latitude(),
                    event.longitude(),
                    Math.round(distance),
                    Instant.ofEpochMilli(event.timestamp()),
                    Instant.now(clock));
            String outboxEntry = OutboxMessage.serialize(objectMapper, topics.geofenceAlerts(), event.vehicleId(), alert);
            if (stateRepository.transition(fence.id(), event.vehicleId(), candidate, outboxEntry)) {
                alerts.add(alert);
                metrics.geofenceTransition(fence.id(), candidate.name());
                log.info("지오펜스 {} vehicleId={} geofence={} distance={}m",
                        candidate, event.vehicleId(), fence.id(), Math.round(distance));
            }
        }
        return alerts;
    }
}
