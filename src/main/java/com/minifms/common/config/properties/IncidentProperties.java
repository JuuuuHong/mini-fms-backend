package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * C-ITS 돌발상황 전파 설정.
 *
 * @param alertRadiusKm  돌발 지점에서 이 반경 안에 있는 주행 차량에 경고를 보낸다.
 * @param alertTtl       경고 유효 시간. 차량 단말은 expiresAt 이 지난 경고를 표시하지 않는다.
 * @param maxRecipients  한 돌발상황으로 경고를 보내는 최대 차량 수.
 * @param publishTimeout 경고 발행 확인을 기다리는 최대 시간.
 */
@ConfigurationProperties(prefix = "fms.incident")
public record IncidentProperties(
        @DefaultValue("2.0") double alertRadiusKm,
        @DefaultValue("10m") Duration alertTtl,
        @DefaultValue("500") int maxRecipients,
        @DefaultValue("3s") Duration publishTimeout) {
}
