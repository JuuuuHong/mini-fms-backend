package com.minifms.simulator;

import com.minifms.common.geo.GeoDistance;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.telematics.domain.TelematicsEvent;
import java.util.Random;

/**
 * 원형 경로를 도는 가상 차량. 경로 중심을 물류센터에서 조금 떨어뜨려,
 * 한 바퀴 도는 동안 물류센터 지오펜스(반경 1km) 경계를 넘나들게 한다.
 */
public final class SimulatedVehicle {

    private static final double BATTERY_USE_PER_KM = 0.2;

    private final String vehicleId;
    private final double depotLatitude;
    private final double depotLongitude;
    private final double routeCenterNorth;
    private final double routeCenterEast;
    private final double routeRadiusMeters;
    private final double cruiseSpeedKmh;
    private final Random random;

    private double angle;
    private double batterySoc;
    private double speedKmh;
    private EngineStatus engineStatus = EngineStatus.ON;
    private int idleTicks;
    private long engineStoppedUntil;

    SimulatedVehicle(String vehicleId, double depotLatitude, double depotLongitude, double centerBearingDeg,
                     double centerDistanceMeters, double routeRadiusMeters, double cruiseSpeedKmh, long seed) {
        this.vehicleId = vehicleId;
        this.depotLatitude = depotLatitude;
        this.depotLongitude = depotLongitude;
        double bearing = Math.toRadians(centerBearingDeg);
        this.routeCenterNorth = centerDistanceMeters * Math.cos(bearing);
        this.routeCenterEast = centerDistanceMeters * Math.sin(bearing);
        this.routeRadiusMeters = routeRadiusMeters;
        this.cruiseSpeedKmh = cruiseSpeedKmh;
        this.random = new Random(seed);
        this.angle = random.nextDouble() * 2 * Math.PI;
        this.batterySoc = 60 + random.nextDouble() * 40;
    }

    public String vehicleId() {
        return vehicleId;
    }

    /**
     * 경과 시간만큼 움직이고 텔레메트리를 만든다.
     *
     * @param speedMultiplier 데모 가속 배율(이동 거리에만 적용)
     */
    public synchronized TelematicsEvent advance(double elapsedSeconds, long nowMillis, double speedMultiplier) {
        if (nowMillis < engineStoppedUntil) {
            speedKmh = 0;
            engineStatus = EngineStatus.OFF;
        } else if (idleTicks > 0) {
            idleTicks--;
            speedKmh = 0;
            engineStatus = EngineStatus.IDLE;
        } else {
            if (random.nextDouble() < 0.02) {
                idleTicks = 3 + random.nextInt(6); // 신호 대기
            }
            speedKmh = clamp(cruiseSpeedKmh + random.nextGaussian() * 4, 5, 110);
            engineStatus = EngineStatus.ON;
            double meters = speedKmh / 3.6 * elapsedSeconds * speedMultiplier;
            angle = (angle + meters / routeRadiusMeters) % (2 * Math.PI);
            batterySoc -= meters / 1000.0 * BATTERY_USE_PER_KM;
            if (batterySoc < 10) {
                batterySoc = 100; // 배터리 교체(스왑)로 가정
            }
        }
        double[] position = position();
        return new TelematicsEvent(vehicleId, round(position[0], 6), round(position[1], 6),
                round(speedKmh, 1), round(batterySoc, 1), engineStatus, nowMillis);
    }

    /** 원격 ENGINE_STOP 실행. until 까지 시동이 꺼진 채 멈춰 있는다. */
    public synchronized void stopEngine(long untilMillis) {
        engineStoppedUntil = untilMillis;
    }

    /** 경로 위에서 물류센터까지의 최소·최대 거리(m). 경계를 넘나드는지 확인하는 데 쓴다. */
    public double[] depotDistanceRangeMeters() {
        double centerDistance = Math.hypot(routeCenterNorth, routeCenterEast);
        return new double[] {Math.abs(centerDistance - routeRadiusMeters), centerDistance + routeRadiusMeters};
    }

    private double[] position() {
        double north = routeCenterNorth + routeRadiusMeters * Math.cos(angle);
        double east = routeCenterEast + routeRadiusMeters * Math.sin(angle);
        return GeoDistance.offset(depotLatitude, depotLongitude, north, east);
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private static double round(double value, int scale) {
        double factor = Math.pow(10, scale);
        return Math.round(value * factor) / factor;
    }
}
