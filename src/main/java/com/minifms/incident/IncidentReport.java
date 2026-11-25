package com.minifms.incident;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * RSU·센서가 올리는 돌발상황 보고.
 *
 * @param incidentId RSU 가 붙인 고유 ID. 같은 보고가 재전송돼도 경고를 한 번만 보내는 멱등키다.
 * @param sourceId   보고한 RSU·센서 ID
 * @param occurredAt 발생 시각(epoch ms)
 */
public record IncidentReport(
        @NotBlank @Size(max = 64) @Pattern(regexp = "^[A-Za-z0-9_-]+$") String incidentId,
        @NotNull IncidentType type,
        @NotNull @DecimalMin("-85.05112878") @DecimalMax("85.05112878") Double latitude,
        @NotNull @DecimalMin("-180.0") @DecimalMax("180.0") Double longitude,
        @NotBlank @Size(max = 64) String sourceId,
        @NotNull @Positive Long occurredAt,
        @Size(max = 200) String description) {
}
