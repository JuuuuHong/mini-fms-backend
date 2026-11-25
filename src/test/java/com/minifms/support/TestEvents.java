package com.minifms.support;

import com.minifms.common.geo.GeoDistance;
import com.minifms.telematics.domain.EngineStatus;
import com.minifms.telematics.domain.TelematicsEvent;
import java.util.UUID;

/** 테스트 이벤트 생성 도우미. */
public final class TestEvents {

    private TestEvents() {
    }

    public static String uniqueId(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** 기준점에서 북쪽으로 meters 떨어진 위치의 이벤트. */
    public static TelematicsEvent northOf(String vehicleId, double latitude, double longitude, double meters,
                                          long timestamp) {
        return at(vehicleId, latitude, longitude, meters, 0, EngineStatus.ON, 42.0, timestamp);
    }

    public static TelematicsEvent at(String vehicleId, double latitude, double longitude, double northMeters,
                                     double eastMeters, EngineStatus engineStatus, double speed, long timestamp) {
        double[] position = GeoDistance.offset(latitude, longitude, northMeters, eastMeters);
        return new TelematicsEvent(vehicleId, position[0], position[1], speed, 80.0, engineStatus, timestamp);
    }
}
