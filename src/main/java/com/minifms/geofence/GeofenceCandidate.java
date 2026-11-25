package com.minifms.geofence;

/**
 * 이벤트 하나로 판정한 지오펜스 전이 후보. 이전 상태와 다를 때만 실제 전이가 되며,
 * 그 판단과 알림 적재는 위치 갱신 Lua 안에서 원자적으로 한다.
 *
 * @param outboxEntry 전이가 일어나면 아웃박스에 넣을 알림(JSON)
 */
public record GeofenceCandidate(String geofenceId, GeofenceTransition transition, GeofenceAlert alert, String outboxEntry) {
}
