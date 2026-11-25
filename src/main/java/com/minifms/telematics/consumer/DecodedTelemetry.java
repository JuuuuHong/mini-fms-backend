package com.minifms.telematics.consumer;

import com.minifms.telematics.domain.TelematicsEvent;

/** Kafka 메시지 해석 결과. event 와 error 중 하나만 있다. */
public record DecodedTelemetry(TelematicsEvent event, String error) {

    public static DecodedTelemetry valid(TelematicsEvent event) {
        return new DecodedTelemetry(event, null);
    }

    public static DecodedTelemetry invalid(String error) {
        return new DecodedTelemetry(null, error);
    }

    public boolean isValid() {
        return event != null;
    }
}
