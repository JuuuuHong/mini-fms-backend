package com.minifms.ride;

import com.minifms.ride.fare.Fare;
import com.minifms.ride.fare.Tariff;
import com.minifms.ride.zone.ZoneType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** 라이드 저장소(PostgreSQL). 반납 확정은 IN_PROGRESS 일 때만 바꾸는 조건부 갱신이다. */
@Repository
public class RideRepository {

    private final JdbcClient jdbc;

    public RideRepository(JdbcClient rideJdbcClient) {
        this.jdbc = rideJdbcClient;
    }

    /** 사용자·기기당 진행 중 라이드는 하나(유니크 인덱스). 어기면 DuplicateKeyException. */
    public void insert(Ride ride) {
        Tariff tariff = ride.tariff();
        jdbc.sql("""
                        INSERT INTO rides (ride_id, user_id, device_id, status, started_at, unlock_fee, per_minute_fee,
                                           subscription_plan, unlock_fee_waived, riding_discount_percent)
                        VALUES (:rideId, :userId, :deviceId, :status, :startedAt, :unlockFee, :perMinuteFee,
                                :plan, :waived, :percent)""")
                .param("rideId", ride.rideId())
                .param("userId", ride.userId())
                .param("deviceId", ride.deviceId())
                .param("status", ride.status().name())
                .param("startedAt", Timestamp.from(ride.startedAt()))
                .param("unlockFee", tariff.unlockFee())
                .param("perMinuteFee", tariff.perMinuteFee())
                .param("plan", tariff.subscriptionPlan())
                .param("waived", tariff.unlockFeeWaived())
                .param("percent", tariff.ridingDiscountPercent())
                .update();
    }

    public Optional<Ride> find(UUID rideId) {
        return jdbc.sql("SELECT * FROM rides WHERE ride_id = :rideId")
                .param("rideId", rideId)
                .query(RideRepository::map)
                .optional();
    }

    public Optional<UUID> findActiveRideId(String userId) {
        return jdbc.sql("SELECT ride_id FROM rides WHERE user_id = :userId AND status = 'IN_PROGRESS'")
                .param("userId", userId)
                .query(UUID.class)
                .optional();
    }

    /** IN_PROGRESS → COMPLETED(반납 판정·요금 기록). 이미 확정됐으면 false. */
    public boolean complete(UUID rideId, RideReturn ret, Fare fare) {
        return jdbc.sql("""
                        UPDATE rides SET status = 'COMPLETED', ended_at = :endedAt,
                               end_latitude = :lat, end_longitude = :lng, location_measured_at = :measuredAt,
                               zone_id = :zoneId, zone_type = :zoneType,
                               riding_minutes = :minutes, riding_fee = :ridingFee, zone_adjustment = :zoneAdjustment,
                               subscription_discount = :subscriptionDiscount, total_fare = :total
                        WHERE ride_id = :rideId AND status = 'IN_PROGRESS'""")
                .param("rideId", rideId)
                .param("endedAt", Timestamp.from(ret.endedAt()))
                .param("lat", ret.latitude())
                .param("lng", ret.longitude())
                .param("measuredAt", Timestamp.from(ret.locationMeasuredAt()))
                .param("zoneId", ret.zoneId())
                .param("zoneType", ret.zoneType().name())
                .param("minutes", fare.ridingMinutes())
                .param("ridingFee", fare.ridingFee())
                .param("zoneAdjustment", fare.zoneAdjustment())
                .param("subscriptionDiscount", fare.subscriptionDiscount())
                .param("total", fare.total())
                .update() == 1;
    }

    private static Ride map(ResultSet rs, int rowNum) throws SQLException {
        Tariff tariff = new Tariff(
                rs.getLong("unlock_fee"),
                rs.getLong("per_minute_fee"),
                rs.getString("subscription_plan"),
                rs.getBoolean("unlock_fee_waived"),
                rs.getInt("riding_discount_percent"));
        RideStatus status = RideStatus.valueOf(rs.getString("status"));
        RideReturn ret = null;
        Fare fare = null;
        if (status == RideStatus.COMPLETED) {
            ret = new RideReturn(
                    instant(rs.getTimestamp("ended_at")),
                    rs.getDouble("end_latitude"),
                    rs.getDouble("end_longitude"),
                    instant(rs.getTimestamp("location_measured_at")),
                    rs.getString("zone_id"),
                    ZoneType.valueOf(rs.getString("zone_type")));
            fare = new Fare(
                    tariff.unlockFee(),
                    tariff.perMinuteFee(),
                    rs.getLong("riding_minutes"),
                    rs.getLong("riding_fee"),
                    rs.getLong("zone_adjustment"),
                    tariff.subscriptionPlan(),
                    rs.getLong("subscription_discount"),
                    rs.getLong("total_fare"));
        }
        return new Ride(
                rs.getObject("ride_id", UUID.class),
                rs.getString("user_id"),
                rs.getString("device_id"),
                status,
                instant(rs.getTimestamp("started_at")),
                tariff,
                ret,
                fare);
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
