package com.minifms.simulator;

import com.minifms.common.config.properties.SimulatorProperties;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * 가상 차량 묶음(V-001 ~ V-N). 텔레메트리 발생기와 TCU 응답기(ENGINE_STOP 반영)가 함께 쓴다.
 */
@Component
public class SimulatedFleet {

    /** 경로 중심을 물류센터에서 900m 떨어뜨리고 반지름을 300~750m 로 두면, 모든 차량이 반경 1km 경계를 넘나든다. */
    private static final double ROUTE_CENTER_DISTANCE_METERS = 900;

    private final Map<String, SimulatedVehicle> vehicles;

    public SimulatedFleet(SimulatorProperties properties) {
        SimulatorProperties.Telemetry telemetry = properties.telemetry();
        Map<String, SimulatedVehicle> fleet = new LinkedHashMap<>();
        int count = telemetry.vehicleCount();
        for (int i = 0; i < count; i++) {
            String vehicleId = String.format("V-%03d", i + 1);
            fleet.put(vehicleId, new SimulatedVehicle(
                    vehicleId,
                    telemetry.depotLatitude(),
                    telemetry.depotLongitude(),
                    360.0 * i / Math.max(1, count),
                    ROUTE_CENTER_DISTANCE_METERS,
                    300 + (i % 4) * 150,
                    35 + (i % 5) * 6,
                    1_000L + i));
        }
        this.vehicles = Collections.unmodifiableMap(fleet);
    }

    public Collection<SimulatedVehicle> vehicles() {
        return vehicles.values();
    }

    public void stopEngine(String vehicleId, long untilMillis) {
        SimulatedVehicle vehicle = vehicles.get(vehicleId);
        if (vehicle != null) {
            vehicle.stopEngine(untilMillis);
        }
    }
}
