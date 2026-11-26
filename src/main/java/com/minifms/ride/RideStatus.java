package com.minifms.ride;

/** IN_PROGRESS ──반납 확정(1회)──▶ COMPLETED. 반납 거절(구역·위치)이면 IN_PROGRESS 그대로다. */
public enum RideStatus {
    IN_PROGRESS,
    COMPLETED
}
