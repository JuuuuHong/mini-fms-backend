package com.minifms.incident;

import com.minifms.common.config.properties.IncidentProperties;
import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.ClaimResult;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.kafka.KafkaEventPublisher;
import com.minifms.common.web.ConflictException;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.vehicle.location.NearbyVehicle;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * C-ITS 돌발상황 전파: 돌발 지점 반경(기본 2km) 안의 주행 중 차량을 Redis GEO 로 찾아
 * c-its-incident-alerts 로 경고를 브로드캐스트한다.
 *
 * <ul>
 *   <li>"주행 중" = 최근 수신이 있고(활성) 시동이 꺼지지 않은(ON·IDLE) 차량. 신호 대기로 멈춘 차량도 위험 구간에 있으므로 포함한다.</li>
 *   <li>RSU 는 같은 보고를 재전송할 수 있으므로 incidentId 로 멱등 처리한다(텔레메트리와 같은 {@link IdempotencyGuard}).</li>
 *   <li>발행이 일부 실패하면 선점을 풀고 503 을 돌려 RSU 가 재시도하게 한다. 이때 이미 받은 차량은 같은 경고를 다시 받을 수 있어,
 *       단말은 incidentId 로 중복 표시를 거른다.</li>
 * </ul>
 */
@Service
public class IncidentBroadcastService {

    static final String NAMESPACE = "incident";
    private static final Logger log = LoggerFactory.getLogger(IncidentBroadcastService.class);

    private final VehicleLocationRepository locationRepository;
    private final IdempotencyGuard idempotencyGuard;
    private final KafkaEventPublisher publisher;
    private final TopicProperties topics;
    private final IncidentProperties properties;
    private final LocationProperties locationProperties;
    private final Clock clock;

    public IncidentBroadcastService(VehicleLocationRepository locationRepository, IdempotencyGuard idempotencyGuard,
                                    KafkaEventPublisher publisher, TopicProperties topics, IncidentProperties properties,
                                    LocationProperties locationProperties, Clock clock) {
        this.locationRepository = locationRepository;
        this.idempotencyGuard = idempotencyGuard;
        this.publisher = publisher;
        this.topics = topics;
        this.properties = properties;
        this.locationProperties = locationProperties;
        this.clock = clock;
    }

    public BroadcastResult broadcast(IncidentReport report) {
        ClaimResult claim = idempotencyGuard.claim(NAMESPACE, List.of(report.incidentId()));
        switch (claim.status(0)) {
            case DUPLICATE -> {
                log.info("이미 전파한 돌발상황 재보고를 무시합니다 incidentId={}", report.incidentId());
                return new BroadcastResult(report.incidentId(), true, 0, List.of());
            }
            case IN_FLIGHT -> throw new ConflictException("같은 돌발상황을 처리 중입니다",
                    Map.of("incidentId", report.incidentId()));
            case CLAIMED -> {
                // 아래에서 처리
            }
        }

        try {
            long activeSince = clock.millis() - locationProperties.activeThreshold().toMillis();
            List<NearbyVehicle> recipients = locationRepository.searchNearby(
                            report.latitude(), report.longitude(), properties.alertRadiusKm(),
                            activeSince, properties.maxRecipients())
                    .stream()
                    .filter(vehicle -> vehicle.engineStatus() != EngineStatus.OFF)
                    .toList();

            Instant issuedAt = Instant.now(clock);
            List<CompletableFuture<SendResult<String, String>>> sends = recipients.stream()
                    .map(vehicle -> publisher.publish(topics.incidentAlerts(), vehicle.vehicleId(),
                            toAlert(report, vehicle, issuedAt)))
                    .toList();
            publisher.awaitAll(sends, properties.publishTimeout());
            idempotencyGuard.complete(claim, claim.claimedKeys());

            List<String> vehicleIds = recipients.stream().map(NearbyVehicle::vehicleId).toList();
            log.info("돌발상황 전파 incidentId={} type={} 반경 {}km 주행 차량 {}대",
                    report.incidentId(), report.type(), properties.alertRadiusKm(), vehicleIds.size());
            return new BroadcastResult(report.incidentId(), false, vehicleIds.size(), vehicleIds);
        } catch (RuntimeException e) {
            idempotencyGuard.release(claim, claim.claimedKeys());
            throw e;
        }
    }

    private IncidentAlert toAlert(IncidentReport report, NearbyVehicle vehicle, Instant issuedAt) {
        long distanceMeters = Math.round(vehicle.distanceKm() * 1000);
        String message = String.format("%s 발생: 약 %s 앞 구간 주의 운전", report.type().label(), humanDistance(distanceMeters));
        return new IncidentAlert(
                UUID.randomUUID().toString(),
                report.incidentId(),
                vehicle.vehicleId(),
                report.type(),
                report.latitude(),
                report.longitude(),
                distanceMeters,
                report.sourceId(),
                message,
                Instant.ofEpochMilli(report.occurredAt()),
                issuedAt,
                issuedAt.plus(properties.alertTtl()));
    }

    private static String humanDistance(long meters) {
        return meters >= 1000 ? String.format("%.1fkm", meters / 1000.0) : meters + "m";
    }

    /** @param duplicate 이미 전파한 incidentId 의 재보고였으면 true(경고를 다시 보내지 않음) */
    public record BroadcastResult(String incidentId, boolean duplicate, int recipients, List<String> vehicleIds) {
    }
}
