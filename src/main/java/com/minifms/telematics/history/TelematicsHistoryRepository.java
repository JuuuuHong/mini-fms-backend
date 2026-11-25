package com.minifms.telematics.history;

import com.minifms.telematics.domain.TelematicsEvent;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 과거 궤적 저장소(ClickHouse). 스키마는 src/main/resources/clickhouse/schema.sql.
 *
 * <p>시각은 epoch ms 정수로 주고받는다. DateTime64(3) 열에 정수를 넣으면 ClickHouse 는 ms 단위로 해석하고,
 * 읽을 때는 toUnixTimestamp64Milli 로 꺼내므로 JDBC 드라이버의 시간대 변환이 끼어들 여지가 없다.
 */
@Repository
public class TelematicsHistoryRepository {

    private static final String INSERT_SQL = """
            INSERT INTO telematics_history
                (vehicle_id, event_time, latitude, longitude, speed, battery_soc, engine_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)""";

    /**
     * FINAL: 아직 병합되지 않은 중복 행을 조회 시점에 합친다(정렬 키 범위가 좁아 비용이 작다).
     * speed·battery_soc 는 Float32 로 저장하므로 double 로 바꿀 때 생기는 꼬리(81.30000305…)를 반올림한다.
     */
    private static final String TRACK_SQL = """
            SELECT toUnixTimestamp64Milli(event_time) AS ts, latitude, longitude,
                   round(toFloat64(speed), 2) AS speed, round(toFloat64(battery_soc), 2) AS battery_soc, engine_status
            FROM telematics_history FINAL
            WHERE vehicle_id = ?
              AND event_time >= fromUnixTimestamp64Milli(toInt64(?))
              AND event_time < fromUnixTimestamp64Milli(toInt64(?))
            ORDER BY event_time
            LIMIT ?""";

    private final JdbcTemplate jdbcTemplate;

    public TelematicsHistoryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertBatch(List<TelematicsEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(INSERT_SQL, events, events.size(), (ps, event) -> {
            ps.setString(1, event.vehicleId());
            ps.setLong(2, event.timestamp());
            ps.setDouble(3, event.latitude());
            ps.setDouble(4, event.longitude());
            ps.setFloat(5, event.speed().floatValue());
            ps.setFloat(6, event.batterySoc().floatValue());
            ps.setString(7, event.engineStatus().name());
        });
    }

    public List<TrackPoint> findTrack(String vehicleId, Instant from, Instant to, int limit) {
        return jdbcTemplate.query(TRACK_SQL, (rs, rowNum) -> new TrackPoint(
                        Instant.ofEpochMilli(rs.getLong("ts")),
                        rs.getDouble("latitude"),
                        rs.getDouble("longitude"),
                        rs.getDouble("speed"),
                        rs.getDouble("battery_soc"),
                        rs.getString("engine_status")),
                vehicleId, from.toEpochMilli(), to.toEpochMilli(), limit);
    }

    /** 저장된 행 수(병합 전 중복 포함). 멱등 처리 검증용. */
    public long countRaw(String vehicleId) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT count() FROM telematics_history WHERE vehicle_id = ?", Long.class, vehicleId);
        return count == null ? 0 : count;
    }
}
