package com.minifms.ride;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.minifms.common.config.properties.RideProperties;
import com.minifms.common.lock.DistributedLock;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.web.ConflictException;
import com.minifms.ride.device.DeviceState;
import com.minifms.ride.fare.Fare;
import com.minifms.ride.fare.FareCalculator;
import com.minifms.ride.zone.ReturnZoneIndex;
import com.minifms.ride.zone.ZoneType;
import com.minifms.support.TestEvents;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 같은 기기·라이드에 대한 경합. 분산 락이 경합을 줄이고, 락을 잃은 경우에도 DB 조건부 갱신·유니크 인덱스가
 * 상태 전이를 한 번으로 막는지 확인한다.
 */
class RideConcurrencyIT extends RideTestSupport {

    static final int THREADS = 16;

    @Autowired
    RideService rideService;
    @Autowired
    RideTransactions transactions;
    @Autowired
    RideRepository rideRepository;
    @Autowired
    FareCalculator fareCalculator;
    @Autowired
    RedissonClient redisson;
    @Autowired
    DistributedLock distributedLock;
    @Autowired
    SubscriptionRepository subscriptions;
    @Autowired
    ReturnZoneIndex zoneIndex;
    @Autowired
    RideProperties rideProperties;
    @Autowired
    FmsMetrics metrics;
    @Autowired
    Clock clock;

    @Test
    void 같은_기기를_동시에_대여하면_한_명만_성공한다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);

        List<Object> results = race(THREADS, i -> rideService.rent(TestEvents.uniqueId("user"), deviceId, null));

        assertThat(results.stream().filter(Ride.class::isInstance)).hasSize(1);
        assertThat(results.stream().filter(ConflictException.class::isInstance)).hasSize(THREADS - 1);
        Ride winner = (Ride) results.stream().filter(Ride.class::isInstance).findFirst().orElseThrow();
        assertThat(device(deviceId).state()).isEqualTo(DeviceState.IN_USE);
        assertThat(device(deviceId).currentRideId()).isEqualTo(winner.rideId());
        assertThat(activeRides(deviceId)).isEqualTo(1);
    }

    @Test
    void 같은_라이드를_동시에_반납하면_한_번만_확정되고_모두_같은_요금을_받는다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), PENALTY);
        String userId = TestEvents.uniqueId("user");
        UUID rideId = rent(userId, deviceId);
        backdateStart(rideId, Duration.ofMinutes(3));

        List<Object> results = race(THREADS, i -> rideService.returnRide(userId, rideId));

        assertThat(results).allMatch(Ride.class::isInstance);
        assertThat(results.stream().map(r -> ((Ride) r).fare()).distinct()).hasSize(1);
        assertThat(results.stream().map(r -> ((Ride) r).returnInfo().endedAt()).distinct()).hasSize(1);
        assertThat(paymentOutboxRows(rideId)).as("결제 요청").isEqualTo(1);
    }

    @Test
    void 분산_락을_잃어도_반납_확정과_결제_요청은_한_번이다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        String userId = TestEvents.uniqueId("user");
        UUID rideId = rent(userId, deviceId);
        Ride ride = rideRepository.find(rideId).orElseThrow();

        // 락 없이 트랜잭션을 직접 동시에 부른다(락이 만료돼 두 인스턴스가 함께 들어온 상황).
        List<Object> results = race(THREADS, i -> {
            Instant now = Instant.now().plusMillis(i);
            Fare fare = fareCalculator.calculate(ride.tariff(), Duration.between(ride.startedAt(), now), ZoneType.SERVICE_ZONE);
            return transactions.complete(ride, new RideReturn(now, SERVICE[0], SERVICE[1], now, "T-SERVICE",
                    ZoneType.SERVICE_ZONE), fare, now);
        });

        assertThat(results.stream().filter(Boolean.TRUE::equals)).hasSize(1);
        assertThat(results.stream().filter(Boolean.FALSE::equals)).hasSize(THREADS - 1);
        assertThat(paymentOutboxRows(rideId)).isEqualTo(1);
        assertThat(rideRepository.find(rideId).orElseThrow().status()).isEqualTo(RideStatus.COMPLETED);
        assertThat(device(deviceId).state()).isIn(DeviceState.LOCKED, DeviceState.AVAILABLE);
    }

    @Test
    void 락을_잃은_사이_다른_인스턴스가_먼저_확정했으면_그_결과를_돌려주고_결제_요청은_한_건이다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), PENALTY);
        String userId = TestEvents.uniqueId("user");
        UUID rideId = rent(userId, deviceId);
        backdateStart(rideId, Duration.ofMinutes(3));
        Ride ride = rideRepository.find(rideId).orElseThrow();

        // 이 요청이 위치를 보고 요금을 계산하는 사이, 락을 잃어 함께 들어온 다른 인스턴스가 1분 먼저(서비스 구역으로) 확정한다.
        Instant earlier = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MICROS);
        Fare otherFare = fareCalculator.calculate(ride.tariff(), Duration.between(ride.startedAt(), earlier),
                ZoneType.SERVICE_ZONE);
        RideReturn otherReturn = new RideReturn(earlier, SERVICE[0], SERVICE[1], earlier, "T-SERVICE", ZoneType.SERVICE_ZONE);
        RideTransactions racing = spy(transactions);
        doAnswer(invocation -> {
            assertThat(transactions.complete(ride, otherReturn, otherFare, earlier)).isTrue();
            return invocation.callRealMethod();
        }).when(racing).complete(any(), any(), any(), any());
        RideService service = new RideService(distributedLock, racing, rideRepository, deviceRepository, subscriptions,
                locationRepository, zoneIndex, fareCalculator, rideProperties, metrics, clock);

        Ride result = service.returnRide(userId, rideId);

        // 이 요청의 확정(벌금 구역)은 조건부 갱신에서 졌고, 먼저 확정된 결과를 그대로 돌려준다.
        verify(racing).complete(any(), any(), any(), any());
        assertThat(result.status()).isEqualTo(RideStatus.COMPLETED);
        assertThat(result.returnInfo().zoneType()).isEqualTo(ZoneType.SERVICE_ZONE);
        assertThat(result.fare()).isEqualTo(otherFare);
        assertThat(paymentOutboxRows(rideId)).isEqualTo(1);
    }

    @Test
    void 분산_락을_잃어도_한_기기는_한_번만_대여되고_한_사용자는_라이드를_하나만_가진다() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        Instant now = Instant.now();
        List<Object> sameDevice = race(THREADS, i -> {
            transactions.start(Ride.start(UUID.randomUUID(), TestEvents.uniqueId("user"), deviceId, now,
                    fareCalculator.tariffFor(null)));
            return "started";
        });
        assertThat(sameDevice.stream().filter("started"::equals)).hasSize(1);
        assertThat(activeRides(deviceId)).isEqualTo(1);

        // 같은 사용자가 서로 다른 기기를 동시에 빌린다(기기 락이 서로 달라 함께 통과) → 유니크 인덱스가 하나만 받는다.
        String userId = TestEvents.uniqueId("user");
        List<String> devices = new ArrayList<>();
        for (int i = 0; i < THREADS; i++) {
            devices.add(registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE));
        }
        List<Object> sameUser = race(THREADS, i -> rideService.rent(userId, devices.get(i), null));
        assertThat(sameUser.stream().filter(Ride.class::isInstance)).hasSize(1);
        assertThat(sameUser.stream().filter(ConflictException.class::isInstance))
                .allSatisfy(e -> assertThat(((ConflictException) e).properties()).containsEntry("reason", "ACTIVE_RIDE_EXISTS"))
                .hasSize(THREADS - 1);
        assertThat(devices.stream().filter(id -> device(id).state() == DeviceState.IN_USE)).hasSize(1);
    }

    @Test
    void 다른_요청이_기기_락을_쥐고_있으면_기다리다가_409() throws Exception {
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        RLock held = redisson.getLock(RideService.deviceLockKey(deviceId));
        ExecutorService holder = Executors.newSingleThreadExecutor();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> holding = holder.submit(() -> {
            held.lock();
            locked.countDown();
            release.await();
            held.unlock();
            return null;
        });
        try {
            assertThat(locked.await(5, TimeUnit.SECONDS)).isTrue();
            long started = System.nanoTime();
            assertThatThrownBy(() -> rideService.rent(TestEvents.uniqueId("user"), deviceId, null))
                    .isInstanceOfSatisfying(ConflictException.class, e ->
                            assertThat(e.properties()).isEqualTo(Map.of("deviceId", deviceId, "reason", "REQUEST_IN_PROGRESS")));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).as("lock-wait(1초)만큼 기다린다")
                    .isGreaterThanOrEqualTo(Duration.ofMillis(900));
            assertThat(device(deviceId).state()).isEqualTo(DeviceState.AVAILABLE);
        } finally {
            release.countDown();
            holding.get(5, TimeUnit.SECONDS);
            holder.shutdown();
        }
        // 락이 풀리면 대여된다.
        assertThat(rideService.rent(TestEvents.uniqueId("user"), deviceId, null).status()).isEqualTo(RideStatus.IN_PROGRESS);
    }

    private long activeRides(String deviceId) {
        return rideJdbcClient.sql("SELECT count(*) FROM rides WHERE device_id = :id AND status = 'IN_PROGRESS'")
                .param("id", deviceId).query(Long.class).single();
    }

    /** n 개 작업을 동시에 출발시키고 결과(값 또는 예외)를 모은다. */
    private static List<Object> race(int n, IndexedTask task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Object>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int index = i;
                Callable<Object> call = () -> {
                    start.await();
                    try {
                        return task.run(index);
                    } catch (RuntimeException e) {
                        return e;
                    }
                };
                futures.add(pool.submit(call));
            }
            start.countDown();
            List<Object> results = new ArrayList<>();
            for (Future<Object> future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface IndexedTask {
        Object run(int index) throws Exception;
    }
}
