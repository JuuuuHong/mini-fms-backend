package com.minifms.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import com.minifms.common.geo.GeoDistance;
import org.junit.jupiter.api.Test;

class GeoDistanceTest {

    @Test
    void 서울시청에서_부산시청까지_약_325km() {
        double meters = GeoDistance.haversineMeters(37.5665, 126.9780, 35.1796, 129.0756);
        assertThat(meters / 1000).isBetween(322.0, 328.0);
    }

    @Test
    void offset_으로_옮긴_거리를_haversine_이_되돌려_잰다() {
        double[] moved = GeoDistance.offset(37.5665, 126.9780, 600, 800);
        assertThat(GeoDistance.haversineMeters(37.5665, 126.9780, moved[0], moved[1])).isCloseTo(1_000.0, within(1.0));
    }
}
