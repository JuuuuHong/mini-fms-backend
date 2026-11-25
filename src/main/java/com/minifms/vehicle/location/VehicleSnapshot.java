package com.minifms.vehicle.location;

import com.minifms.telematics.domain.EngineStatus;
import java.time.Instant;
import java.util.Map;

/**
 * Redis 에 있는 차량 최신 상태.
 *
 * @param timestamp 단말 측정 시각
 * @param updatedAt 서버가 반영한 시각
 */
public record VehicleSnapshot(
        String vehicleId,
        double latitude,
        double longitude,
        double speed,
        double batterySoc,
        EngineStatus engineStatus,
        Instant timestamp,
        Instant updatedAt) {

    static VehicleSnapshot fromHash(Map<String, String> hash) {
        return new VehicleSnapshot(
                hash.get("vehicleId"),
                Double.parseDouble(hash.get("latitude")),
                Double.parseDouble(hash.get("longitude")),
                Double.parseDouble(hash.get("speed")),
                Double.parseDouble(hash.get("batterySoc")),
                EngineStatus.valueOf(hash.get("engineStatus")),
                Instant.ofEpochMilli(Long.parseLong(hash.get("timestamp"))),
                Instant.ofEpochMilli(Long.parseLong(hash.get("updatedAt"))));
    }
}
