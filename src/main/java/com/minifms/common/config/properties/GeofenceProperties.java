package com.minifms.common.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * 지오펜스(원형 영역) 정의. 운영에서는 DB·관리 화면에서 읽겠지만, 이 프로젝트는 설정 파일로 둔다.
 */
@Validated
@ConfigurationProperties(prefix = "fms.geofence")
public record GeofenceProperties(@DefaultValue List<@Valid Fence> fences) {

    /**
     * @param radiusMeters     이 거리 안이면 진입.
     * @param hysteresisMeters 이탈은 radius + hysteresis 를 넘어야 인정한다. 경계에서 GPS 가 흔들려
     *                         진입·이탈 알림이 반복되는 것(flapping)을 막는 완충 구간이다.
     */
    public record Fence(
            @NotBlank @Pattern(regexp = "^[A-Za-z0-9_-]+$") String id,
            @NotBlank String name,
            @NotNull Double latitude,
            @NotNull Double longitude,
            @Positive double radiusMeters,
            @DefaultValue("30") @PositiveOrZero double hysteresisMeters) {
    }
}
