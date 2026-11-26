package com.minifms.common.config.properties;

import com.minifms.ride.zone.ZoneType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * 반납 구역(다각형). 운영에서는 관리 화면·DB 에서 읽겠지만 지오펜스처럼 설정 파일로 둔다.
 * 다각형의 모양(닫힘·자기 교차·좌표 범위)은 ReturnZoneIndex 가 기동할 때 검사하고, 틀리면 기동하지 않는다.
 */
@Validated
@ConfigurationProperties(prefix = "fms.return-zones")
public record ReturnZoneProperties(@DefaultValue List<@Valid Zone> zones) {

    /**
     * @param polygon WKT POLYGON 또는 MULTIPOLYGON. 좌표 순서는 "경도 위도"(x y)다.
     */
    public record Zone(
            @NotBlank @Pattern(regexp = "^[A-Za-z0-9_-]{1,64}$") String id,
            @NotBlank String name,
            @NotNull ZoneType type,
            @NotBlank String polygon) {
    }
}
