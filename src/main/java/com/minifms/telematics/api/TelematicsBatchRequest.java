package com.minifms.telematics.api;

import com.minifms.telematics.domain.TelematicsEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;

public record TelematicsBatchRequest(
        @NotEmpty @Size(max = 1000) List<@Valid @NotNull TelematicsEvent> events) {
}
