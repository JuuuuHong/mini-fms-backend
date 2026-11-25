package com.minifms.telematics.ingest;

import com.minifms.common.config.properties.TelematicsProperties;
import com.minifms.telematics.domain.TelematicsEvent;
import jakarta.validation.Validator;
import java.time.Clock;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * 수집 API 와 Kafka 소비자가 같은 규칙으로 검증하도록 묶었다.
 * (Kafka 로는 수집 API 를 거치지 않은 다른 생산자의 메시지도 들어올 수 있다.)
 */
@Component
public class TelemetryValidator {

    private final Validator validator;
    private final Clock clock;
    private final TelematicsProperties properties;

    public TelemetryValidator(Validator validator, Clock clock, TelematicsProperties properties) {
        this.validator = validator;
        this.clock = clock;
        this.properties = properties;
    }

    /** 위반 사항 목록. 비어 있으면 유효하다. */
    public List<String> violations(TelematicsEvent event) {
        List<String> violations = validator.validate(event).stream()
                .map(v -> v.getPropertyPath() + ": " + v.getMessage())
                .sorted()
                .toList();
        if (!violations.isEmpty()) {
            return violations;
        }
        long now = clock.millis();
        if (event.timestamp() > now + properties.maxFutureSkew().toMillis()) {
            return List.of("timestamp: 서버 시각보다 " + properties.maxFutureSkew() + " 넘게 앞섭니다(단말 시계 확인 필요)");
        }
        if (event.timestamp() < now - properties.maxPastAge().toMillis()) {
            return List.of("timestamp: " + properties.maxPastAge() + " 보다 오래된 측정값은 받지 않습니다(단말 오프라인 버퍼 허용 범위 밖)");
        }
        return List.of();
    }
}
