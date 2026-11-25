package com.minifms.vehicle.location;

import com.minifms.geofence.GeofenceCandidate;
import java.util.List;

/**
 * 위치 갱신 결과.
 *
 * @param applied     더 최신이라 반영했으면 true(같거나 과거 시각이면 false, 이때 지오펜스도 그대로다)
 * @param transitions 함께 일어난 지오펜스 전이(알림이 아웃박스에 들어간 것)
 */
public record LocationUpdate(boolean applied, List<GeofenceCandidate> transitions) {

    public static final LocationUpdate IGNORED = new LocationUpdate(false, List.of());
}
