package com.minifms.telematics.domain;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * 차량 단말이 1초 주기로 보내는 텔레메트리 한 건. Kafka 메시지 본문(JSON)과 수집 API 본문이 같은 모양이다.
 *
 * @param vehicleId    Redis 키에 들어가므로 영문·숫자·_·- 만 허용한다.
 * @param latitude     Redis GEO 가 색인할 수 있는 범위(±85.05112878)로 제한한다.
 * @param speed        km/h
 * @param batterySoc   배터리 잔량(%)
 * @param timestamp    단말 측정 시각(epoch ms). vehicleId 와 함께 멱등키가 된다.
 */
public record TelematicsEvent(
        @NotBlank @Size(max = 64) @Pattern(regexp = "^[A-Za-z0-9_-]+$", message = "영문·숫자·_·- 만 쓸 수 있습니다")
        String vehicleId,
        @NotNull @DecimalMin("-85.05112878") @DecimalMax("85.05112878")
        Double latitude,
        @NotNull @DecimalMin("-180.0") @DecimalMax("180.0")
        Double longitude,
        @NotNull @DecimalMin("0.0") @DecimalMax("400.0")
        Double speed,
        @NotNull @DecimalMin("0.0") @DecimalMax("100.0")
        Double batterySoc,
        @NotNull
        EngineStatus engineStatus,
        @NotNull @Positive
        Long timestamp) {

    /** 같은 차량이 같은 측정 시각(ms)에 보낸 메시지는 재전송으로 보고 하나만 처리한다. */
    public String idempotencyKey() {
        return vehicleId + ":" + timestamp;
    }
}
