package com.minifms.ride.zone;

/**
 * 반납 좌표의 구역 판정 결과.
 *
 * @param zoneId   고른 구역. 어느 구역에도 속하지 않으면(서비스 지역 밖) null.
 * @param zoneType 고른 구역의 정책. 서비스 지역 밖이면 null.
 */
public record ZoneJudgement(String zoneId, String zoneName, ZoneType zoneType) {

    public static final ZoneJudgement OUT_OF_SERVICE_AREA = new ZoneJudgement(null, null, null);

    public boolean returnable() {
        return zoneType != null && zoneType.returnable();
    }

    /** 반납할 수 없을 때의 사유 코드. 반납할 수 있으면 null. */
    public String rejectionReason() {
        if (zoneType == null) {
            return "OUT_OF_SERVICE_AREA";
        }
        return zoneType.returnable() ? null : "NO_RETURN_ZONE";
    }
}
