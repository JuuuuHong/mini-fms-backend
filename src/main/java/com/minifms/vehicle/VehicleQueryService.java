package com.minifms.vehicle;

import com.minifms.command.VehicleCommandRepository;
import com.minifms.command.domain.ControlState;
import com.minifms.common.config.properties.LocationProperties;
import com.minifms.common.web.InvalidRequestException;
import com.minifms.common.web.NotFoundException;
import com.minifms.telematics.history.TelematicsHistoryRepository;
import com.minifms.telematics.history.TrackPoint;
import com.minifms.vehicle.location.NearbyVehicle;
import com.minifms.vehicle.location.VehicleLocationRepository;
import com.minifms.vehicle.location.VehicleSnapshot;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Service;

/** 조회 전용 서비스: 실시간 상태는 Redis, 과거 궤적은 ClickHouse 에서 읽는다(CQRS 의 조회 측). */
@Service
public class VehicleQueryService {

    private static final Duration MAX_TRACK_RANGE = Duration.ofDays(7);

    private final VehicleLocationRepository locationRepository;
    private final VehicleCommandRepository commandRepository;
    private final TelematicsHistoryRepository historyRepository;
    private final LocationProperties properties;
    private final Clock clock;

    public VehicleQueryService(VehicleLocationRepository locationRepository, VehicleCommandRepository commandRepository,
                               TelematicsHistoryRepository historyRepository, LocationProperties properties, Clock clock) {
        this.locationRepository = locationRepository;
        this.commandRepository = commandRepository;
        this.historyRepository = historyRepository;
        this.properties = properties;
        this.clock = clock;
    }

    public List<NearbyVehicle> findNearby(double latitude, double longitude, double radiusKm, int limit) {
        long activeSince = clock.millis() - properties.activeThreshold().toMillis();
        int effectiveLimit = Math.min(limit, properties.maxNearbyResults());
        return locationRepository.searchNearby(latitude, longitude, radiusKm, activeSince, effectiveLimit);
    }

    public VehicleDetail getDetail(String vehicleId) {
        VehicleSnapshot latest = locationRepository.findLatest(vehicleId)
                .orElseThrow(() -> new NotFoundException("차량을 찾을 수 없습니다: " + vehicleId));
        boolean active = latest.timestamp().isAfter(Instant.now(clock).minus(properties.activeThreshold()));
        return new VehicleDetail(latest, active, commandRepository.findControlState(vehicleId));
    }

    public List<TrackPoint> getTrack(String vehicleId, Instant from, Instant to, int limit) {
        if (!from.isBefore(to)) {
            throw new InvalidRequestException("조회 기간이 올바르지 않습니다", List.of("from 은 to 보다 앞서야 합니다"));
        }
        if (Duration.between(from, to).compareTo(MAX_TRACK_RANGE) > 0) {
            throw new InvalidRequestException("조회 기간이 너무 깁니다", List.of("최대 " + MAX_TRACK_RANGE.toDays() + "일"));
        }
        return historyRepository.findTrack(vehicleId, from, to, limit);
    }

    public record VehicleDetail(VehicleSnapshot latest, boolean active, ControlState controlState) {
    }
}
