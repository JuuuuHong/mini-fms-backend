package com.minifms.telematics.history;

import java.time.Instant;

/** ClickHouse 에서 읽은 과거 궤적의 한 점. */
public record TrackPoint(
        Instant timestamp,
        double latitude,
        double longitude,
        double speed,
        double batterySoc,
        String engineStatus) {
}
