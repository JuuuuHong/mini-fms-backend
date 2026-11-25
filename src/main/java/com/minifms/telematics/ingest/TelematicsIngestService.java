package com.minifms.telematics.ingest;

import com.minifms.common.config.properties.TelematicsProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.kafka.KafkaEventPublisher;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.web.InvalidRequestException;
import com.minifms.telematics.domain.TelematicsEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * 수집 진입점. 검증만 하고 바로 telematics-raw-events 로 넘긴다(저장은 소비자가 한다).
 * 수집 경로를 가볍게 두어 단말 트래픽이 몰려도 API 가 저장소 지연에 끌려가지 않게 한다.
 */
@Service
public class TelematicsIngestService {

    private static final Logger log = LoggerFactory.getLogger(TelematicsIngestService.class);

    private final KafkaEventPublisher publisher;
    private final TelemetryValidator validator;
    private final TopicProperties topics;
    private final TelematicsProperties properties;
    private final FmsMetrics metrics;

    public TelematicsIngestService(KafkaEventPublisher publisher, TelemetryValidator validator,
                                   TopicProperties topics, TelematicsProperties properties, FmsMetrics metrics) {
        this.publisher = publisher;
        this.validator = validator;
        this.topics = topics;
        this.properties = properties;
        this.metrics = metrics;
    }

    /** API 수집: 모두 검증한 뒤 한꺼번에 보내고 브로커 확인까지 기다린다(202 는 "유실되지 않음"을 뜻한다). */
    public int ingest(List<TelematicsEvent> events) {
        List<String> violations = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            int index = i;
            validator.violations(events.get(i)).forEach(v -> violations.add("events[" + index + "]." + v));
        }
        if (!violations.isEmpty()) {
            throw new InvalidRequestException("텔레메트리 검증 실패", violations);
        }
        List<CompletableFuture<SendResult<String, String>>> sends = events.stream()
                .map(event -> publisher.publish(topics.telematicsRaw(), event.vehicleId(), event))
                .toList();
        publisher.awaitAll(sends, properties.publishTimeout());
        metrics.ingested(events.size());
        return events.size();
    }

    /** 시뮬레이터 수집: 확인을 기다리지 않는다(1초마다 새 값이 오므로 실패는 로그만 남긴다). */
    public void publishAsync(TelematicsEvent event) {
        publisher.publish(topics.telematicsRaw(), event.vehicleId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("텔레메트리 발행 실패 vehicleId={} ts={}: {}",
                                event.vehicleId(), event.timestamp(), ex.getMessage());
                    } else {
                        metrics.ingested(1);
                    }
                });
    }
}
