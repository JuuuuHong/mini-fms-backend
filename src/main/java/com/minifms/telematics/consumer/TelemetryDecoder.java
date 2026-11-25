package com.minifms.telematics.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.telematics.ingest.TelemetryValidator;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * 역직렬화를 Kafka 역직렬화기가 아니라 여기서 한다. 깨진 메시지(poison pill)가 컨테이너 단계에서
 * 무한 재시도되지 않고, 배치 안에서 "무효"로 분류돼 나머지 메시지 처리를 막지 않게 하기 위해서다.
 */
@Component
public class TelemetryDecoder {

    private final ObjectMapper objectMapper;
    private final TelemetryValidator validator;

    public TelemetryDecoder(ObjectMapper objectMapper, TelemetryValidator validator) {
        this.objectMapper = objectMapper;
        this.validator = validator;
    }

    public DecodedTelemetry decode(String payload) {
        if (payload == null || payload.isBlank()) {
            return DecodedTelemetry.invalid("EMPTY_PAYLOAD");
        }
        try {
            TelematicsEvent event = objectMapper.readValue(payload, TelematicsEvent.class);
            if (event == null) {
                return DecodedTelemetry.invalid("EMPTY_PAYLOAD");
            }
            List<String> violations = validator.violations(event);
            return violations.isEmpty()
                    ? DecodedTelemetry.valid(event)
                    : DecodedTelemetry.invalid("INVALID_FIELDS: " + String.join(", ", violations));
        } catch (JsonProcessingException e) {
            return DecodedTelemetry.invalid("MALFORMED_PAYLOAD: " + e.getOriginalMessage());
        }
    }
}
