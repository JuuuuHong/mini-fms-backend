package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 텔레매틱스 수집 설정. 받을 수 있는 측정 시각의 범위는 단말에 대한 가정이다.
 *
 * @param maxPastAge     이보다 오래된 측정값은 거부한다. 단말이 통신 두절 동안 버퍼에 모아 두었다가 올리는
 *                       기간에 대한 가정(초기값 7일). 범위를 묶어 두면 한 묶음이 수많은 날짜(ClickHouse 파티션)에
 *                       퍼져 이력 저장이 막히는 일도 막는다.
 * @param maxFutureSkew  서버 시각보다 이만큼 넘게 앞선 측정값은 단말 시계 오류로 보고 거부한다(초기값 30초).
 *                       앞선 시각이 한 건 들어오면 그 시각까지 해당 차량의 최신 위치 갱신이 멈추므로 작게 둔다.
 * @param publishTimeout 수집 API 가 Kafka 브로커 확인(acks=all)을 기다리는 최대 시간.
 */
@ConfigurationProperties(prefix = "fms.telematics")
public record TelematicsProperties(
        @DefaultValue("7d") Duration maxPastAge,
        @DefaultValue("30s") Duration maxFutureSkew,
        @DefaultValue("3s") Duration publishTimeout) {
}
