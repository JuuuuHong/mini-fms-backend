package com.minifms.ride;

import com.minifms.common.config.properties.RideProperties;
import com.minifms.common.lock.DistributedLock;
import com.minifms.common.lock.LockUnavailableException;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.web.ConflictException;
import com.minifms.common.web.NotFoundException;
import com.minifms.common.web.UnprocessableException;
import com.minifms.ride.device.Device;
import com.minifms.ride.device.DeviceRepository;
import com.minifms.ride.device.DeviceState;
import com.minifms.ride.fare.Fare;
import com.minifms.ride.fare.FareCalculator;
import com.minifms.ride.fare.Tariff;
import com.minifms.ride.zone.ReturnZoneIndex;
import com.minifms.ride.zone.ZoneJudgement;
import com.minifms.vehicle.location.VehicleLocationRepository;
import com.minifms.vehicle.location.VehicleSnapshot;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

/**
 * 대여·반납 오케스트레이션.
 *
 * <pre>
 * 대여: [기기 락] 기기·사용자·단말 위치(신선도) 확인 → 요금표·구독권 스냅샷 → TX{ 기기 AVAILABLE→IN_USE, 라이드 생성 }
 * 반납: [기기 락] 단말 최신 위치(신선도) → 구역 판정 → 요금 계산
 *              → TX{ 라이드 COMPLETED, 기기 IN_USE→LOCKED, 결제 요청 아웃박스 }
 *       이후 잠금 명령 발송·확인은 DeviceLockReconciler, 결제 요청 발행은 PaymentOutboxRelay 가 비동기로 한다.
 * </pre>
 *
 * 같은 기기에 대한 대여·반납은 Redisson 락으로 한 번에 하나씩 처리한다(중복 반납·동시 대여 경합). 락이 경합 중에 풀려도
 * DB 의 조건부 갱신과 유니크 인덱스가 전이를 한 번으로 막는다. 반납은 멱등이다 — 이미 확정된 라이드는 저장된 결과를 돌려준다.
 */
@Service
public class RideService {

    private static final Logger log = LoggerFactory.getLogger(RideService.class);

    private final DistributedLock lock;
    private final RideTransactions transactions;
    private final RideRepository rides;
    private final DeviceRepository devices;
    private final SubscriptionRepository subscriptions;
    private final VehicleLocationRepository locations;
    private final ReturnZoneIndex zoneIndex;
    private final FareCalculator fareCalculator;
    private final RideProperties properties;
    private final FmsMetrics metrics;
    private final Clock clock;

    public RideService(DistributedLock lock, RideTransactions transactions, RideRepository rides,
                       DeviceRepository devices, SubscriptionRepository subscriptions,
                       VehicleLocationRepository locations, ReturnZoneIndex zoneIndex, FareCalculator fareCalculator,
                       RideProperties properties, FmsMetrics metrics, Clock clock) {
        this.lock = lock;
        this.transactions = transactions;
        this.rides = rides;
        this.devices = devices;
        this.subscriptions = subscriptions;
        this.locations = locations;
        this.zoneIndex = zoneIndex;
        this.fareCalculator = fareCalculator;
        this.properties = properties;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** 대여 락 키. 대여·반납이 같은 키를 써서 한 기기에 대한 요청을 줄 세운다. */
    public static String deviceLockKey(String deviceId) {
        return "fms:lock:device:" + deviceId;
    }

    public Ride rent(String userId, String deviceId) {
        return withDeviceLock(deviceId, () -> {
            Device device = devices.find(deviceId)
                    .orElseThrow(() -> new NotFoundException("등록되지 않은 기기입니다: " + deviceId));
            if (device.state() != DeviceState.AVAILABLE) {
                throw new ConflictException("이미 대여 중이거나 대여할 수 없는 기기입니다.",
                        Map.of("deviceId", deviceId, "state", device.state().name(), "reason", "DEVICE_NOT_AVAILABLE"));
            }
            rides.findActiveRideId(userId).ifPresent(active -> {
                throw activeRideConflict(active);
            });

            Instant now = clock.instant();
            // 반납은 단말의 최신 위치로만 판정한다. 지금 위치를 보고하지 않는 기기를 빌려주면 반납할 수 없는 라이드가 생기고
            // 그동안 주행 요금이 쌓이므로, 반납과 같은 신선도 기준을 대여에도 건다.
            freshLocation(deviceId, now, "대여");
            Tariff tariff = fareCalculator.tariffFor(subscriptions.findActivePlanCodes(userId, now));
            Ride ride = Ride.start(UUID.randomUUID(), userId, deviceId, now, tariff);
            try {
                transactions.start(ride);
            } catch (DuplicateKeyException e) {
                // 같은 사용자가 다른 기기를 동시에 대여(기기 락이 서로 달라 함께 통과)하면 DB 유니크 인덱스가 하나만 받는다.
                throw activeRideConflict(rides.findActiveRideId(userId).orElse(null));
            }
            metrics.rideStarted();
            log.info("대여 rideId={} userId={} deviceId={} subscription={}", ride.rideId(), userId, deviceId,
                    tariff.subscriptionPlan());
            return ride;
        });
    }

    /**
     * 반납. 판정에 쓰는 좌표는 요청이 아니라 단말이 보낸 최신 텔레메트리다(클라이언트가 좌표를 꾸밀 수 없게).
     *
     * @throws UnprocessableException 위치를 확인할 수 없거나 반납할 수 없는 구역(라이드는 진행 중 그대로)
     */
    public Ride returnRide(String userId, UUID rideId) {
        Ride ride = findOwned(rideId, userId);
        if (ride.status() == RideStatus.COMPLETED) {
            return ride; // 멱등: 이미 확정된 반납은 같은 결과
        }
        return withDeviceLock(ride.deviceId(), () -> {
            Ride current = findOwned(rideId, userId);
            if (current.status() == RideStatus.COMPLETED) {
                return current; // 락을 기다리는 사이 앞선 요청이 확정했다
            }
            Instant now = clock.instant();
            VehicleSnapshot location;
            try {
                location = freshLocation(current.deviceId(), now, "반납");
            } catch (UnprocessableException e) {
                metrics.rideReturnRejected(String.valueOf(e.properties().get("reason")));
                throw e;
            }
            ZoneJudgement zone = zoneIndex.judge(location.latitude(), location.longitude());
            if (!zone.returnable()) {
                metrics.rideReturnRejected(zone.rejectionReason());
                Map<String, Object> details = new HashMap<>();
                details.put("reason", zone.rejectionReason());
                details.put("latitude", location.latitude());
                details.put("longitude", location.longitude());
                if (zone.zoneId() != null) {
                    details.put("zoneId", zone.zoneId());
                    details.put("zoneName", zone.zoneName());
                }
                throw new UnprocessableException(zone.zoneType() == null
                        ? "서비스 지역 밖에서는 반납할 수 없습니다." : "반납할 수 없는 구역입니다: " + zone.zoneName(), details);
            }

            Fare fare = fareCalculator.calculate(current.tariff(), Duration.between(current.startedAt(), now), zone.zoneType());
            RideReturn ret = new RideReturn(now, location.latitude(), location.longitude(), location.timestamp(),
                    zone.zoneId(), zone.zoneType());
            if (transactions.complete(current, ret, fare, now)) {
                metrics.rideReturned(zone.zoneType().name());
                log.info("반납 rideId={} deviceId={} zone={}({}) minutes={} total={}", rideId, current.deviceId(),
                        zone.zoneId(), zone.zoneType(), fare.ridingMinutes(), fare.total());
            } else {
                log.warn("다른 요청이 먼저 반납을 확정했습니다(분산 락 경합) rideId={}", rideId);
            }
            return rides.find(rideId).orElseThrow();
        });
    }

    /** fleetView 면 누구의 라이드든, 아니면 본인 라이드만(남의 라이드는 존재 여부도 알리지 않고 404). */
    public Ride get(UUID rideId, String userId, boolean fleetView) {
        return fleetView
                ? rides.find(rideId).orElseThrow(() -> new NotFoundException("라이드를 찾을 수 없습니다: " + rideId))
                : findOwned(rideId, userId);
    }

    private Ride findOwned(UUID rideId, String userId) {
        return rides.find(rideId)
                .filter(ride -> ride.userId().equals(userId))
                .orElseThrow(() -> new NotFoundException("라이드를 찾을 수 없습니다: " + rideId));
    }

    /** 단말이 maxLocationAge 안에 보고한 최신 위치. 없거나 오래됐으면 422(action = "대여" | "반납"). */
    private VehicleSnapshot freshLocation(String deviceId, Instant now, String action) {
        VehicleSnapshot location = locations.findLatest(deviceId).orElseThrow(() -> new UnprocessableException(
                "기기 위치를 확인할 수 없어 " + action + "할 수 없습니다. 잠시 뒤 다시 시도하세요.",
                Map.of("reason", "LOCATION_UNAVAILABLE")));
        Duration age = Duration.between(location.timestamp(), now);
        if (age.compareTo(properties.maxLocationAge()) > 0) {
            throw new UnprocessableException("기기의 최근 위치가 오래돼 " + action + "할 수 없습니다. 잠시 뒤 다시 시도하세요.",
                    Map.of("reason", "LOCATION_STALE", "locationMeasuredAt", location.timestamp().toString(),
                            "maxLocationAgeSeconds", properties.maxLocationAge().toSeconds()));
        }
        return location;
    }

    private <T> T withDeviceLock(String deviceId, Supplier<T> action) {
        try {
            return lock.executeWithLock(deviceLockKey(deviceId), properties.lockWait(), action);
        } catch (LockUnavailableException e) {
            throw new ConflictException("같은 기기에 대한 다른 요청을 처리 중입니다. 잠시 뒤 다시 시도하세요.",
                    Map.of("deviceId", deviceId, "reason", "REQUEST_IN_PROGRESS"));
        }
    }

    private static ConflictException activeRideConflict(UUID activeRideId) {
        Map<String, Object> details = new HashMap<>();
        details.put("reason", "ACTIVE_RIDE_EXISTS");
        if (activeRideId != null) {
            details.put("activeRideId", activeRideId.toString());
        }
        return new ConflictException("이미 이용 중인 라이드가 있습니다. 반납한 뒤 다시 대여하세요.", details);
    }
}
