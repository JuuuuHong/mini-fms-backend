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
 * 지오펜스 진입·이탈 판정.
 *
 * <p>판정 규칙(히스테리시스):
 * <pre>
 *   거리 ≤ radius                 → 안(ENTER 후보)
 *   거리 > radius + hysteresis    → 밖(EXIT 후보)
 *   그 사이(완충 구간)            → 후보 없음(상태 유지)
 * </pre>
 * 이 클래스는 후보만 만든다. 후보가 이전 상태와 다를 때의 전이·알림 적재는 최신 위치 갱신과 같은 Lua 스크립트가
 * 원자적으로 한다({@code VehicleLocationRepository#applyIfNewer}). 그래서 위치만 반영되고 알림이 빠지는 일이 없고,
 * 최신 위치를 갱신하지 못한(늦게 도착한) 이벤트는 지오펜스 상태도 바꾸지 않는다.
 *
 * <p>지오펜스 수가 많아지면 모든 펜스와 거리를 재는 대신 공간 색인(펜스 중심 GEO 인덱스로 후보 추리기)이 필요하다.
 */
@Component
public class GeofenceDetector {

    private static final Logger log = LoggerFactory.getLogger(GeofenceDetector.class);

    private final GeofenceProperties properties;
    private final TopicProperties topics;
    private final ObjectMapper objectMapper;
    private final FmsMetrics metrics;
    private final Clock clock;

    public GeofenceDetector(GeofenceProperties properties, TopicProperties topics, ObjectMapper objectMapper,
                            FmsMetrics metrics, Clock clock) {
        this.properties = properties;
        this.topics = topics;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** 이 이벤트 위치에서의 지오펜스별 전이 후보(완충 구간에 있는 펜스는 빠진다). */
    public List<GeofenceCandidate> candidates(TelematicsEvent event) {
        List<GeofenceCandidate> candidates = new ArrayList<>();
        for (Fence fence : properties.fences()) {
            double distance = GeoDistance.haversineMeters(
                    fence.latitude(), fence.longitude(), event.latitude(), event.longitude());
            GeofenceTransition transition;
            if (distance <= fence.radiusMeters()) {
                transition = GeofenceTransition.ENTER;
            } else if (distance > fence.radiusMeters() + fence.hysteresisMeters()) {
                transition = GeofenceTransition.EXIT;
            } else {
                continue;
            }
            GeofenceAlert alert = new GeofenceAlert(
                    UUID.randomUUID().toString(),
                    event.vehicleId(),
                    fence.id(),
                    fence.name(),
                    transition,
                    event.latitude(),
                    event.longitude(),
                    Math.round(distance),
                    Instant.ofEpochMilli(event.timestamp()),
                    Instant.now(clock));
            candidates.add(new GeofenceCandidate(fence.id(), transition, alert,
                    OutboxMessage.serialize(objectMapper, topics.geofenceAlerts(), event.vehicleId(), alert)));
        }
        return candidates;
    }

    /** 실제로 일어난 전이를 지표·로그로 남긴다. */
    public void recordTransitions(List<GeofenceCandidate> transitions) {
        for (GeofenceCandidate transition : transitions) {
            metrics.geofenceTransition(transition.geofenceId(), transition.transition().name());
            log.info("지오펜스 {} vehicleId={} geofence={} distance={}m", transition.transition(),
                    transition.alert().vehicleId(), transition.geofenceId(), transition.alert().distanceMeters());
        }
    }
}
