package com.minifms.ride.device;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * 기기 상태 저장소(PostgreSQL). 상태 전이는 모두 "현재 상태가 기대한 값일 때만" 바꾸는 조건부 갱신이고,
 * 바꿨는지(영향 받은 행 수)를 돌려준다. 분산 락을 잃은 경합에서도 전이는 한 번만 일어난다.
 */
@Repository
public class DeviceRepository {

    private static final String COLUMNS = """
            device_id, state, current_ride_id, lock_command_id, lock_attempts, lock_retry_at, state_changed_at, registered_at""";

    private final JdbcClient jdbc;

    public DeviceRepository(JdbcClient rideJdbcClient) {
        this.jdbc = rideJdbcClient;
    }

    /** AVAILABLE 로 등록한다. 이미 있으면 그대로 두고 false. */
    public boolean register(String deviceId, Instant now) {
        return jdbc.sql("""
                        INSERT INTO devices (device_id, state, state_changed_at, registered_at)
                        VALUES (:deviceId, 'AVAILABLE', :now, :now)
                        ON CONFLICT (device_id) DO NOTHING""")
                .param("deviceId", deviceId)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    public Optional<Device> find(String deviceId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM devices WHERE device_id = :deviceId")
                .param("deviceId", deviceId)
                .query(DeviceRepository::map)
                .optional();
    }

    /** AVAILABLE → IN_USE. */
    public boolean markInUse(String deviceId, UUID rideId, Instant now) {
        return jdbc.sql("""
                        UPDATE devices SET state = 'IN_USE', current_ride_id = :rideId, state_changed_at = :now
                        WHERE device_id = :deviceId AND state = 'AVAILABLE'""")
                .param("deviceId", deviceId)
                .param("rideId", rideId)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /** IN_USE(그 라이드) → LOCKED. 잠금 조정기가 바로 잠금 명령을 보내도록 lock_retry_at = now. */
    public boolean markLocked(String deviceId, UUID rideId, Instant now) {
        return jdbc.sql("""
                        UPDATE devices SET state = 'LOCKED', current_ride_id = NULL, lock_command_id = NULL,
                                           lock_attempts = 0, lock_retry_at = :now, state_changed_at = :now
                        WHERE device_id = :deviceId AND state = 'IN_USE' AND current_ride_id = :rideId""")
                .param("deviceId", deviceId)
                .param("rideId", rideId)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /** 잠금 조정 차례가 된 LOCKED 기기(오래 기다린 순). */
    public List<Device> findLockDue(Instant now, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM devices
                        WHERE state = 'LOCKED' AND lock_retry_at <= :now
                        ORDER BY lock_retry_at
                        LIMIT :limit""")
                .param("now", Timestamp.from(now))
                .param("limit", limit)
                .query(DeviceRepository::map)
                .list();
    }

    /**
     * 잠금 명령을 보내기 전에 그 명령 ID 로 기기 행을 선점한다(LOCKED 이고 진행 중 명령이 없을 때만).
     * 선점하지 못했으면(그사이 상태가 바뀜) 명령을 보내면 안 된다 — 오래된 스냅샷으로 이미 대여된 기기를 잠그지 않기 위해서다.
     */
    public boolean claimLockCommand(String deviceId, String commandId, Instant checkAt) {
        return jdbc.sql("""
                        UPDATE devices SET lock_command_id = :commandId, lock_attempts = lock_attempts + 1,
                                           lock_retry_at = :checkAt
                        WHERE device_id = :deviceId AND state = 'LOCKED' AND lock_command_id IS NULL""")
                .param("deviceId", deviceId)
                .param("commandId", commandId)
                .param("checkAt", Timestamp.from(checkAt))
                .update() == 1;
    }

    /** 명령을 보내지 못해 선점을 푼다(보낸 명령이 아니므로 시도 수를 되돌린다). */
    public boolean releaseLockClaim(String deviceId, String commandId, Instant retryAt) {
        return jdbc.sql("""
                        UPDATE devices SET lock_command_id = NULL, lock_attempts = lock_attempts - 1, lock_retry_at = :retryAt
                        WHERE device_id = :deviceId AND state = 'LOCKED' AND lock_command_id = :commandId""")
                .param("deviceId", deviceId)
                .param("commandId", commandId)
                .param("retryAt", Timestamp.from(retryAt))
                .update() == 1;
    }

    /** 잠금이 확인됐다: LOCKED(그 명령) → AVAILABLE. */
    public boolean markAvailable(String deviceId, String commandId, Instant now) {
        return jdbc.sql("""
                        UPDATE devices SET state = 'AVAILABLE', lock_command_id = NULL, lock_attempts = 0,
                                           lock_retry_at = NULL, state_changed_at = :now
                        WHERE device_id = :deviceId AND state = 'LOCKED' AND lock_command_id = :commandId""")
                .param("deviceId", deviceId)
                .param("commandId", commandId)
                .param("now", Timestamp.from(now))
                .update() == 1;
    }

    /** 잠금 명령이 실패했다: 명령을 비우고 retryAt 에 다시 보낸다. */
    public boolean clearFailedLockCommand(String deviceId, String commandId, Instant retryAt) {
        return jdbc.sql("""
                        UPDATE devices SET lock_command_id = NULL, lock_retry_at = :retryAt
                        WHERE device_id = :deviceId AND state = 'LOCKED' AND lock_command_id = :commandId""")
                .param("deviceId", deviceId)
                .param("commandId", commandId)
                .param("retryAt", Timestamp.from(retryAt))
                .update() == 1;
    }

    /** 다음 확인 시각만 미룬다(명령 진행 중·다른 명령 대기·일시 오류). commandId 가 그대로일 때만. */
    public boolean postponeLockCheck(String deviceId, String commandId, Instant checkAt) {
        return jdbc.sql("""
                        UPDATE devices SET lock_retry_at = :checkAt
                        WHERE device_id = :deviceId AND state = 'LOCKED'
                          AND lock_command_id IS NOT DISTINCT FROM CAST(:commandId AS VARCHAR)""")
                .param("deviceId", deviceId)
                .param("commandId", commandId)
                .param("checkAt", Timestamp.from(checkAt))
                .update() == 1;
    }

    private static Device map(ResultSet rs, int rowNum) throws SQLException {
        return new Device(
                rs.getString("device_id"),
                DeviceState.valueOf(rs.getString("state")),
                rs.getObject("current_ride_id", UUID.class),
                rs.getString("lock_command_id"),
                rs.getInt("lock_attempts"),
                instant(rs.getTimestamp("lock_retry_at")),
                instant(rs.getTimestamp("state_changed_at")),
                instant(rs.getTimestamp("registered_at")));
    }

    static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
