package com.minifms.common.geo;

/**
 * 구면 거리 계산. 지구 반지름은 Redis GEO 와 같은 값을 써서 GEOSEARCH 결과와 지오펜스 판정이 어긋나지 않게 한다.
 */
public final class GeoDistance {

    /** Redis geohash 구현(geohash_helper.c)이 쓰는 지구 반지름(m). */
    public static final double EARTH_RADIUS_METERS = 6_372_797.560856;

    private GeoDistance() {
    }

    public static double haversineMeters(double lat1, double lng1, double lat2, double lng2) {
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dPhi = Math.toRadians(lat2 - lat1);
        double dLambda = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
        return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    /**
     * 기준점에서 북쪽·동쪽으로 미터만큼 옮긴 좌표. 수 km 이내에서는 평면 근사로 충분하다.
     *
     * @return {latitude, longitude}
     */
    public static double[] offset(double latitude, double longitude, double northMeters, double eastMeters) {
        double dLat = Math.toDegrees(northMeters / EARTH_RADIUS_METERS);
        double dLng = Math.toDegrees(eastMeters / (EARTH_RADIUS_METERS * Math.cos(Math.toRadians(latitude))));
        return new double[] {latitude + dLat, longitude + dLng};
    }
}
