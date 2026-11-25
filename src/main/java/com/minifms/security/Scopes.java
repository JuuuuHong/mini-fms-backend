package com.minifms.security;

import java.util.List;

/** API 권한(scope). 토큰의 scope 클레임(공백 구분)에 넣으면 Spring Security 가 SCOPE_xxx 권한으로 바꾼다. */
public final class Scopes {

    /** 관제 조회(차량 위치·궤적·지오펜스·지표). */
    public static final String FLEET_READ = "fleet:read";
    /** 차량 원격 제어. */
    public static final String VEHICLE_CONTROL = "vehicle:control";
    /** 텔레메트리 수집(단말·게이트웨이). */
    public static final String TELEMATICS_WRITE = "telematics:write";
    /** 돌발상황 보고(RSU·센서 게이트웨이). */
    public static final String INCIDENT_WRITE = "incident:write";

    public static final List<String> ALL = List.of(FLEET_READ, VEHICLE_CONTROL, TELEMATICS_WRITE, INCIDENT_WRITE);

    private Scopes() {
    }

    public static String authority(String scope) {
        return "SCOPE_" + scope;
    }
}
