package com.minifms.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import com.minifms.common.config.properties.SimulatorProperties;
import com.minifms.common.geo.GeoDistance;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.telematics.domain.TelematicsEvent;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class SimulatedFleetTest {

    static final double DEPOT_LAT = 37.5665;
    static final double DEPOT_LNG = 126.9780;

    final SimulatedFleet fleet = new SimulatedFleet(new SimulatorProperties(
            new SimulatorProperties.Telemetry(true, 10, Duration.ofSeconds(1), 0.05, 5, DEPOT_LAT, DEPOT_LNG),
            new SimulatorProperties.Responder(true, Duration.ofMillis(300), Duration.ofMillis(1500), List.of(), List.of())));

    @Test
    void 차량_10대의_경로가_모두_물류센터_지오펜스_경계_1km_를_넘나든다() {
        assertThat(fleet.vehicles()).hasSize(10);
        assertThat(fleet.vehicles()).allSatisfy(vehicle -> {
            double[] range = vehicle.depotDistanceRangeMeters();
            assertThat(range[0]).isLessThan(1_000);
            assertThat(range[1]).isGreaterThan(1_030); // 완충 구간(30m)까지 넘어야 EXIT 가 난다
        });
    }

    @Test
    void 매_틱마다_이동한_좌표와_측정_시각을_보낸다() {
        SimulatedVehicle vehicle = fleet.vehicles().iterator().next();
        TelematicsEvent first = vehicle.advance(1.0, 1_000_000L, 5);
        TelematicsEvent second = vehicle.advance(1.0, 1_001_000L, 5);

        assertThat(first.vehicleId()).isEqualTo("V-001");
        assertThat(second.timestamp()).isEqualTo(1_001_000L);
        assertThat(GeoDistance.haversineMeters(DEPOT_LAT, DEPOT_LNG, first.latitude(), first.longitude())).isLessThan(2_000);
        assertThat(first.batterySoc()).isBetween(0.0, 100.0);
    }

    @Test
    void 원격_시동_정지를_받으면_멈추고_시동이_꺼진다() {
        fleet.stopEngine("V-002", 2_000_000L);
        SimulatedVehicle vehicle = fleet.vehicles().stream().filter(v -> v.vehicleId().equals("V-002")).findFirst().orElseThrow();

        TelematicsEvent stopped = vehicle.advance(1.0, 1_500_000L, 5);
        assertThat(stopped.engineStatus()).isEqualTo(EngineStatus.OFF);
        assertThat(stopped.speed()).isZero();

        TelematicsEvent resumed = vehicle.advance(1.0, 2_000_001L, 5);
        assertThat(resumed.engineStatus()).isIn(EngineStatus.ON, EngineStatus.IDLE);
    }
}
