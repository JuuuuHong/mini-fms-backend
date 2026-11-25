package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 원격 제어 명령 설정.
 *
 * @param ackTimeout           요청 뒤 이 시간 안에 차량 응답이 없으면 TIMEOUT 으로 보고 보상한다.
 * @param timeoutScanInterval  타임아웃 검사 주기.
 * @param transmitTimeout      명령을 Kafka 에 싣는(전송) 확인을 기다리는 최대 시간.
 * @param lockGrace            차량별 명령 잠금은 ackTimeout + lockGrace 뒤 자동 해제된다(스케줄러가 멈춘 경우의 안전장치).
 * @param recordRetention      명령 기록 보존 기간.
 * @param timeoutScanBatchSize 한 번에 검사하는 만료 후보 수.
 */
@ConfigurationProperties(prefix = "fms.command")
public record CommandProperties(
        @DefaultValue("5s") Duration ackTimeout,
        @DefaultValue("500ms") Duration timeoutScanInterval,
        @DefaultValue("3s") Duration transmitTimeout,
        @DefaultValue("10s") Duration lockGrace,
        @DefaultValue("24h") Duration recordRetention,
        @DefaultValue("100") int timeoutScanBatchSize) {
}
