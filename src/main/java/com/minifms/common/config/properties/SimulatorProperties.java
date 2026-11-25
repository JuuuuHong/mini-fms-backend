package com.minifms.common.config.properties;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 가상 차량(텔레메트리 발생기)과 가상 차량 단말(TCU, 명령 응답기) 설정.
 */
@ConfigurationProperties(prefix = "fms.simulator")
public record SimulatorProperties(@DefaultValue Telemetry telemetry, @DefaultValue Responder responder) {

    /**
     * @param speedMultiplier 데모용 가속 배율. 보고하는 속도(km/h)는 그대로 두고 이동 거리만 배율만큼 늘린다.
     * @param duplicateRate   같은 메시지를 한 번 더 보내는 확률(네트워크 재전송 흉내). 멱등 처리 확인용.
     * @param depotLatitude   경로의 기준점(물류센터). 기본 지오펜스와 같은 좌표다.
     */
    public record Telemetry(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("10") int vehicleCount,
            @DefaultValue("1s") Duration interval,
            @DefaultValue("0.05") double duplicateRate,
            @DefaultValue("5") double speedMultiplier,
            @DefaultValue("37.5665") double depotLatitude,
            @DefaultValue("126.9780") double depotLongitude) {
    }

    /**
     * @param unresponsiveVehicles 명령에 응답하지 않는 차량(타임아웃 시연용).
     * @param rejectingVehicles    명령을 거절하는 차량(실패 보상 시연용).
     */
    public record Responder(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("300ms") Duration minDelay,
            @DefaultValue("1500ms") Duration maxDelay,
            @DefaultValue List<String> unresponsiveVehicles,
            @DefaultValue List<String> rejectingVehicles) {
    }
}
