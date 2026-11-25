package com.minifms.incident;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.IncidentHistoryProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.outbox.OutboxMessage;
import com.minifms.common.redis.RedisKeys;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * 돌발상황 처리 결과를 Redis 아웃박스에 남긴다. 아웃박스 릴레이가 c-its-incident-results 로 발행하고,
 * 색인 소비자가 Elasticsearch 에 반영한다.
 *
 * <p>Kafka 에 직접 보내지 않는 이유: 경고 발행이 실패한 처리(브로커 장애로 FAILED)도 이력에 남아야 하는데,
 * 그 순간 Kafka 는 받지 못한다. 아웃박스에 넣어 두면 브로커가 돌아온 뒤 릴레이가 발행한다.
 */
@Component
public class IncidentResultRecorder {

    private static final RedisScript<Long> NEXT_VERSION =
            RedisScript.of(new ClassPathResource("scripts/incident_next_version.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final TopicProperties topics;
    private final IncidentHistoryProperties properties;
    private final Clock clock;

    public IncidentResultRecorder(StringRedisTemplate redis, ObjectMapper objectMapper, TopicProperties topics,
                                  IncidentHistoryProperties properties, Clock clock) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.topics = topics;
        this.properties = properties;
        this.clock = clock;
    }

    public IncidentProcessingResult record(IncidentReport report, int targetCount, int confirmedCount, int failedCount) {
        Instant processedAt = Instant.now(clock);
        Long version = redis.execute(NEXT_VERSION, List.of(RedisKeys.incidentResultVersion(report.incidentId())),
                Long.toString(processedAt.toEpochMilli()), Long.toString(properties.versionTtl().toMillis()));
        if (version == null) {
            throw new IllegalStateException("처리 결과 버전을 발급하지 못했습니다: " + report.incidentId());
        }
        IncidentProcessingResult result = new IncidentProcessingResult(
                report.incidentId(),
                version,
                report.type(),
                report.sourceId(),
                report.latitude(),
                report.longitude(),
                Instant.ofEpochMilli(report.occurredAt()),
                report.description(),
                IncidentDispatchStatus.of(targetCount, confirmedCount),
                targetCount,
                confirmedCount,
                failedCount,
                processedAt);
        redis.opsForList().rightPush(RedisKeys.OUTBOX,
                OutboxMessage.serialize(objectMapper, topics.incidentResults(), report.incidentId(), result));
        return result;
    }
}
