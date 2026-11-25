package com.minifms.vehicle.api;

import com.minifms.telematics.history.TrackPoint;
import com.minifms.vehicle.VehicleQueryService;
import com.minifms.vehicle.VehicleQueryService.VehicleDetail;
import com.minifms.vehicle.location.NearbyVehicle;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 차량 조회 API. 파라미터 제약은 Spring MVC 내장 메서드 검증이 확인하고, 위반 시 400(Problem Details)을 돌려준다.
 */
@RestController
@RequestMapping("/api/v1/vehicles")
public class VehicleQueryController {

    private static final String VEHICLE_ID_PATTERN = "^[A-Za-z0-9_-]{1,64}$";

    private final VehicleQueryService queryService;
    private final Clock clock;

    public VehicleQueryController(VehicleQueryService queryService, Clock clock) {
        this.queryService = queryService;
        this.clock = clock;
    }

    /** 반경 N km 안의 활성 차량(가까운 순). */
    @GetMapping("/nearby")
    public NearbyResponse nearby(
            @RequestParam @DecimalMin("-85.05112878") @DecimalMax("85.05112878") double lat,
            @RequestParam @DecimalMin("-180.0") @DecimalMax("180.0") double lng,
            @RequestParam @DecimalMin(value = "0.0", inclusive = false) @DecimalMax("100.0") double radiusKm,
            @RequestParam(defaultValue = "100") @Min(1) @Max(500) int limit) {
        List<NearbyVehicle> vehicles = queryService.findNearby(lat, lng, radiusKm, limit);
        return new NearbyResponse(lat, lng, radiusKm, vehicles.size(), vehicles);
    }

    /** 차량 최신 상태 + 원격 제어 상태. */
    @GetMapping("/{vehicleId}")
    public VehicleDetail detail(@PathVariable @Pattern(regexp = VEHICLE_ID_PATTERN) String vehicleId) {
        return queryService.getDetail(vehicleId);
    }

    /** 과거 궤적(기본: 최근 1시간). */
    @GetMapping("/{vehicleId}/track")
    public TrackResponse track(
            @PathVariable @Pattern(regexp = VEHICLE_ID_PATTERN) String vehicleId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "1000") @Min(1) @Max(10_000) int limit) {
        Instant end = to != null ? to : Instant.now(clock);
        Instant start = from != null ? from : end.minus(Duration.ofHours(1));
        List<TrackPoint> points = queryService.getTrack(vehicleId, start, end, limit);
        return new TrackResponse(vehicleId, start, end, points.size(), points);
    }

    public record NearbyResponse(double latitude, double longitude, double radiusKm, int count,
                                 List<NearbyVehicle> vehicles) {
    }

    public record TrackResponse(String vehicleId, Instant from, Instant to, int count, List<TrackPoint> points) {
    }
}
