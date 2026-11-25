package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 텔레매틱스 수집 설정.
 *
 * @param maxFutureSkew  서버 시각보다 이만큼 넘게 앞선 timestamp 는 단말 시계 오류로 보고 거부한다.
 * @param publishTimeout 수집 API 가 Kafka 브로커 확인(acks=all)을 기다리는 최대 시간.
 */
@ConfigurationProperties(prefix = "fms.telematics")
public record TelematicsProperties(
        @DefaultValue("5m") Duration maxFutureSkew,
        @DefaultValue("3s") Duration publishTimeout) {
}
