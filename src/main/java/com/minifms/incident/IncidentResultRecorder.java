package com.minifms.incident;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.IncidentHistoryProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.idempotency.ClaimResult;
import com.minifms.common.outbox.OutboxMessage;
import com.minifms.common.redis.RedisKeys;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * 돌발상황 처리 결과를 Redis 아웃박스에 남긴다. 아웃박스 릴레이가 c-its-incident-results 로 발행하고,
 * 색인 소비자가 Elasticsearch 에 반영한다.
 *
 * <ul>
 *   <li>Kafka 에 직접 보내지 않는 이유: 경고 발행이 실패한 처리(브로커 장애로 FAILED)도 이력에 남아야 하는데,
 *       그 순간 Kafka 는 받지 못한다. 아웃박스에 넣어 두면 브로커가 돌아온 뒤 릴레이가 발행한다.</li>
 *   <li>선점 확인·버전 발급·적재를 Lua 하나로 한다(incident_record_result.lua). 선점이 만료돼 다른 처리가 이어받은 뒤
 *       늦게 끝난 처리는 결과를 남기지 못하므로, 이어받은 처리의 결과를 덮어쓰지 않는다.</li>
 * </ul>
 */
@Component
public class IncidentResultRecorder {

    /** 아웃박스 항목의 버전 자리. Lua 가 발급한 버전으로 바꾼다. */
    static final long VERSION_PLACEHOLDER = -1L;

    private static final RedisScript<Long> RECORD =
            RedisScript.of(new ClassPathResource("scripts/incident_record_result.lua"), Long.class);

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

    /**
     * @param claim 이 처리가 잡은 incidentId 선점
     * @return 기록한 결과. 선점을 이미 잃었으면(만료 뒤 다른 처리가 이어받음) 비어 있다.
     */
    public Optional<IncidentProcessingResult> record(ClaimResult claim, IncidentReport report,
                                                     int targetCount, int confirmedCount, int failedCount) {
        Instant processedAt = Instant.now(clock);
        IncidentProcessingResult draft = result(report, VERSION_PLACEHOLDER, targetCount, confirmedCount, failedCount, processedAt);
        String entry = OutboxMessage.serialize(objectMapper, topics.incidentResults(), report.incidentId(), draft);

        Long version = redis.execute(RECORD,
                List.of(RedisKeys.idempotency(claim.namespace(), report.incidentId()),
                        RedisKeys.incidentResultVersion(report.incidentId()),
                        RedisKeys.OUTBOX),
                claim.token(), Long.toString(processedAt.toEpochMilli()),
                Long.toString(properties.versionTtl().toMillis()), entry);
        if (version == null) {
            throw new IllegalStateException("처리 결과를 기록하지 못했습니다: " + report.incidentId());
        }
        if (version < 0) {
            return Optional.empty();
        }
        return Optional.of(result(report, version, targetCount, confirmedCount, failedCount, processedAt));
    }

    private static IncidentProcessingResult result(IncidentReport report, long version, int targetCount,
                                                   int confirmedCount, int failedCount, Instant processedAt) {
        return new IncidentProcessingResult(
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
    }
}
