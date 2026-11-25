package com.minifms.incident;

import com.minifms.common.config.properties.IncidentProperties;
import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.ClaimResult;
import com.minifms.common.idempotency.IdempotencyGuard;
import com.minifms.common.kafka.EventPublishException;
import com.minifms.common.kafka.KafkaEventPublisher;
import com.minifms.common.web.ConflictException;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.vehicle.location.NearbyVehicle;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * C-ITS 돌발상황 전파: 돌발 지점 반경(기본 2km) 안의 주행 중 차량을 Redis GEO 로 찾아
 * c-its-incident-alerts 로 경고를 브로드캐스트하고, 처리 결과를 관제 이력으로 남긴다.
 *
 * <ul>
 *   <li>"주행 중" = 최근 수신이 있고(활성) 시동이 꺼지지 않은(ON·IDLE) 차량. 신호 대기로 멈춘 차량도 위험 구간에 있으므로 포함한다.</li>
 *   <li>RSU 는 같은 보고를 재전송할 수 있으므로 incidentId 로 멱등 처리한다(텔레메트리와 같은 {@link IdempotencyGuard}).</li>
 *   <li>경고마다 브로커 확인을 따로 기다려 확인 건수를 센다. 대상 0대·전부 확인·일부 실패·전부 실패 모두
 *       처리 결과로 남긴다({@link IncidentResultRecorder} → c-its-incident-results → Elasticsearch).</li>
 *   <li>하나라도 확인되지 않으면 선점을 풀고 503 을 돌려 RSU 가 재시도하게 한다(기존 계약). 재시도는 새 버전의 결과를
 *       남기고, 이미 받은 차량은 같은 경고를 다시 받을 수 있어 단말이 incidentId 로 중복 표시를 거른다.</li>
 *   <li>이력 저장소(Elasticsearch)는 이 경로에 없다. 색인이 멈춰도 접수와 경고 발행은 계속된다.</li>
 * </ul>
 */
@Service
public class IncidentBroadcastService {

    static final String NAMESPACE = "incident";
    private static final Logger log = LoggerFactory.getLogger(IncidentBroadcastService.class);

    private final VehicleLocationRepository locationRepository;
    private final IdempotencyGuard idempotencyGuard;
    private final KafkaEventPublisher publisher;
    private final IncidentResultRecorder resultRecorder;
    private final TopicProperties topics;
    private final IncidentProperties properties;
    private final LocationProperties locationProperties;
    private final Clock clock;

    public IncidentBroadcastService(VehicleLocationRepository locationRepository, IdempotencyGuard idempotencyGuard,
                                    KafkaEventPublisher publisher, IncidentResultRecorder resultRecorder,
                                    TopicProperties topics, IncidentProperties properties,
                                    LocationProperties locationProperties, Clock clock) {
        this.locationRepository = locationRepository;
        this.idempotencyGuard = idempotencyGuard;
        this.publisher = publisher;
        this.resultRecorder = resultRecorder;
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
            // 시동 꺼짐을 먼저 거른 뒤 상한을 적용한다(거르기 전에 자르면 주행 차량이 대상에서 빠질 수 있다).
            List<NearbyVehicle> recipients = locationRepository.searchNearby(
                    report.latitude(), report.longitude(), properties.alertRadiusKm(),
                    activeSince, properties.maxRecipients(), vehicle -> vehicle.engineStatus() != EngineStatus.OFF);

            Dispatch dispatch = dispatch(report, recipients);
            IncidentProcessingResult result = resultRecorder
                    .record(claim, report, recipients.size(), dispatch.confirmed(), dispatch.failed())
                    .orElseThrow(() -> new ConflictException(
                            "처리 시간이 길어 선점이 만료됐고 같은 돌발상황을 다른 처리가 이어받았습니다. 이 처리의 결과는 남기지 않습니다",
                            Map.of("incidentId", report.incidentId())));
            List<String> vehicleIds = recipients.stream().map(NearbyVehicle::vehicleId).toList();
            log.info("돌발상황 전파 incidentId={} type={} 반경 {}km 대상 {}대 확인 {}건 상태 {} version={}",
                    report.incidentId(), report.type(), properties.alertRadiusKm(), recipients.size(),
                    dispatch.confirmed(), result.status(), result.version());

            if (dispatch.failed() > 0) {
                idempotencyGuard.release(claim, claim.claimedKeys());
                throw new EventPublishException("돌발상황 경고 " + recipients.size() + "건 중 "
                        + dispatch.failed() + "건의 발행을 확인하지 못했습니다", dispatch.firstError());
            }
            idempotencyGuard.complete(claim, claim.claimedKeys());
            return new BroadcastResult(report.incidentId(), false, vehicleIds.size(), vehicleIds);
        } catch (EventPublishException e) {
            throw e; // 선점은 위에서 풀었다
        } catch (RuntimeException e) {
            releaseQuietly(claim);
            throw e;
        }
    }

    /**
     * 경고를 보내고, 하나의 마감(publishTimeout) 안에서 건별로 브로커 확인을 기다려 센다.
     * <ul>
     *   <li>마감은 보내기 전에 잡는다. 보내는 동안 마감이 지나면 남은 차량은 보내지 않고 실패로 센다.</li>
     *   <li>send 가 브로커 장애로 즉시 실패하면(메타데이터를 max.block.ms 안에 얻지 못함 등, 재시도 가능한 오류) 남은 차량도
     *       같은 이유로 실패하므로 더 보내지 않는다. 브로커 장애 때 차량마다 max.block.ms 씩 요청 스레드가 묶이는 것을 막는다.
     *       레코드 한 건만의 거절(재시도해도 같은 오류)은 다른 차량과 무관하므로 계속 보낸다.</li>
     * </ul>
     * 그래서 confirmedCount 는 "마감 안에 브로커가 수신을 확인한 수"다.
     */
    private Dispatch dispatch(IncidentReport report, List<NearbyVehicle> recipients) {
        Instant issuedAt = Instant.now(clock);
        long deadline = System.nanoTime() + properties.publishTimeout().toNanos();
        List<CompletableFuture<?>> sends = new ArrayList<>(recipients.size());
        Throwable firstError = null;
        for (NearbyVehicle vehicle : recipients) {
            if (System.nanoTime() >= deadline) {
                firstError = new TimeoutException("발행 마감(" + properties.publishTimeout() + ") 안에 모든 경고를 보내지 못했습니다");
                break;
            }
            CompletableFuture<?> send = publisher.publish(topics.incidentAlerts(), vehicle.vehicleId(),
                    toAlert(report, vehicle, issuedAt));
            sends.add(send);
            if (send.isCompletedExceptionally() && KafkaEventPublisher.isRetriable(send.exceptionNow())) {
                break;
            }
        }

        int confirmed = 0;
        for (CompletableFuture<?> send : sends) {
            try {
                send.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                confirmed++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                firstError = firstError == null ? e : firstError;
                break;
            } catch (ExecutionException e) {
                firstError = firstError == null ? e.getCause() : firstError;
            } catch (TimeoutException e) {
                firstError = firstError == null ? e : firstError;
            }
        }
        return new Dispatch(confirmed, recipients.size() - confirmed, firstError);
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

    private void releaseQuietly(ClaimResult claim) {
        try {
            idempotencyGuard.release(claim, claim.claimedKeys());
        } catch (RuntimeException e) {
            log.warn("돌발상황 멱등키 해제 실패(리스 만료로 풀립니다): {}", e.getMessage());
        }
    }

    private static String humanDistance(long meters) {
        return meters >= 1000 ? String.format("%.1fkm", meters / 1000.0) : meters + "m";
    }

    private record Dispatch(int confirmed, int failed, Throwable firstError) {
    }

    /** @param duplicate 이미 전파한 incidentId 의 재보고였으면 true(경고를 다시 보내지 않음) */
    public record BroadcastResult(String incidentId, boolean duplicate, int recipients, List<String> vehicleIds) {
    }
}
