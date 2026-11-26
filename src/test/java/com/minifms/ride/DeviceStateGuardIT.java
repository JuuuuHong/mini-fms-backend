package com.minifms.ride;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.minifms.common.web.ConflictException;
import com.minifms.ride.device.DeviceState;
import com.minifms.ride.fare.FareCalculator;
import com.minifms.support.TestEvents;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 락이나 서비스의 사전 검사를 거치지 않고 저장소·트랜잭션을 직접 불러도, 조건부 갱신이 기기 상태 기계를 지키는지 본다.
 * (유니크 인덱스가 대신 막는 경우와 구분되도록, 진행 중 라이드가 없는 상태에서 시험한다.)
 */
class DeviceStateGuardIT extends RideTestSupport {

    @Autowired
    RideTransactions transactions;
    @Autowired
    FareCalculator fareCalculator;

    @Test
    void 잠금이_확인되지_않은_LOCKED_기기는_대여_트랜잭션도_받지_않는다() throws Exception {
        // LOCKED 이고 진행 중 라이드가 없다 — 기기 유니크 인덱스는 막아 주지 않으므로 markInUse 의 상태 조건만이 막는다.
        String deviceId = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        lockWithoutSchedule(deviceId);

        UUID rideId = UUID.randomUUID();
        assertThatThrownBy(() -> transactions.start(Ride.start(rideId, TestEvents.uniqueId("user"), deviceId,
                Instant.now(), fareCalculator.tariffFor(null))))
                .isInstanceOfSatisfying(ConflictException.class, e ->
                        assertThat(e.properties()).containsEntry("reason", "DEVICE_NOT_AVAILABLE"));
        assertThat(device(deviceId).state()).isEqualTo(DeviceState.LOCKED);
        assertThat(rideJdbcClient.sql("SELECT count(*) FROM rides WHERE ride_id = :id").param("id", rideId)
                .query(Long.class).single()).isZero();
    }

    @Test
    void 잠금_명령_선점은_LOCKED_이고_진행_중_명령이_없을_때만_된다() throws Exception {
        Instant checkAt = Instant.now().plusSeconds(60);

        // 대여 중(IN_USE) — 오래된 스냅샷으로 주행 중인 기기를 잠그면 안 된다.
        String inUse = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        UUID rideId = rent(TestEvents.uniqueId("user"), inUse);
        assertThat(deviceRepository.claimLockCommand(inUse, UUID.randomUUID().toString(), checkAt)).isFalse();
        assertThat(device(inUse).state()).isEqualTo(DeviceState.IN_USE);
        assertThat(device(inUse).currentRideId()).isEqualTo(rideId);
        assertThat(device(inUse).lockCommandId()).isNull();

        // 대여 가능(AVAILABLE)
        String available = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        assertThat(deviceRepository.claimLockCommand(available, UUID.randomUUID().toString(), checkAt)).isFalse();
        assertThat(device(available).lockCommandId()).isNull();

        // LOCKED: 처음 선점은 되고, 진행 중 명령이 있으면 다른 명령으로는 선점하지 못한다.
        String locked = registerDeviceAt(TestEvents.uniqueId("KB"), SERVICE);
        lockWithoutSchedule(locked);
        String firstCommand = UUID.randomUUID().toString();
        assertThat(deviceRepository.claimLockCommand(locked, firstCommand, checkAt)).isTrue();
        assertThat(deviceRepository.claimLockCommand(locked, UUID.randomUUID().toString(), checkAt)).isFalse();
        assertThat(device(locked).lockCommandId()).isEqualTo(firstCommand);
        assertThat(device(locked).lockAttempts()).isEqualTo(1);
    }

    /** 기기를 LOCKED 로 두되 잠금 조정기가 가져가지 않게 다음 확인 시각을 비운다(조정기는 lock_retry_at 이 지난 기기만 본다). */
    private void lockWithoutSchedule(String deviceId) {
        rideJdbcClient.sql("""
                        UPDATE devices SET state = 'LOCKED', lock_retry_at = NULL, lock_command_id = NULL
                        WHERE device_id = :id AND state = 'AVAILABLE'""")
                .param("id", deviceId).update();
        assertThat(device(deviceId).state()).isEqualTo(DeviceState.LOCKED);
    }
}
