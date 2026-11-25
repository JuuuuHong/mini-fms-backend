package com.minifms.vehicle.location;

import com.minifms.telematics.domain.EngineStatus;
import java.time.Instant;

/** 반경 조회 결과 한 건. distanceKm 는 조회 중심에서의 거리. */
public record NearbyVehicle(
        String vehicleId,
        double latitude,
        double longitude,
        double distanceKm,
        double speed,
        double batterySoc,
        EngineStatus engineStatus,
        Instant lastSeenAt) {
}
