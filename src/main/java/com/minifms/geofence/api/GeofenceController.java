package com.minifms.geofence.api;

import com.minifms.common.config.properties.GeofenceProperties;
import com.minifms.geofence.GeofenceStateRepository;
import java.util.List;
import java.util.TreeSet;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/geofences")
public class GeofenceController {

    private final GeofenceProperties properties;
    private final GeofenceStateRepository stateRepository;

    public GeofenceController(GeofenceProperties properties, GeofenceStateRepository stateRepository) {
        this.properties = properties;
        this.stateRepository = stateRepository;
    }

    /** 지오펜스 목록과 지금 안에 있는 차량. */
    @GetMapping
    public List<GeofenceView> list() {
        return properties.fences().stream()
                .map(fence -> new GeofenceView(
                        fence.id(), fence.name(), fence.latitude(), fence.longitude(),
                        fence.radiusMeters(), fence.hysteresisMeters(),
                        List.copyOf(new TreeSet<>(stateRepository.vehiclesInside(fence.id())))))
                .toList();
    }

    public record GeofenceView(String id, String name, double latitude, double longitude,
                               double radiusMeters, double hysteresisMeters, List<String> vehiclesInside) {
    }
}
