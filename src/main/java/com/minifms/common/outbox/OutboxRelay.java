package com.minifms.common.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.OutboxProperties;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.redis.RedisKeys;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

/**
 * Redis 아웃박스 → Kafka 릴레이(at-least-once).
 *
 * <p>지오펜스 진입·이탈과 명령 보상은 "Redis 상태 변경"과 "이벤트 발행"이 함께 일어나야 한다.
 * 상태를 먼저 바꾸고 Kafka 발행이 실패하면 이벤트를 영영 잃는다(예: 집합에는 들어갔는데 ENTER 알림 없음).
 * 그래서 Lua 스크립트가 상태 변경과 아웃박스 적재를 원자적으로 하고, 발행은 이 릴레이가 재시도한다.
 *
 * <ul>
 *   <li>여러 인스턴스 중 잠금을 가진 하나만 릴레이한다(LTRIM 이 서로의 항목을 지우지 않도록).</li>
 *   <li>앞에서부터 연속으로 발행 확인된 항목만 지운다. 중간에 실패하면 그 뒤는 다음 주기에 다시 보내므로
 *       중복 발행될 수 있다. 소비자는 이벤트의 고유 ID(alertId·eventId)로 중복을 거른다.</li>
 * </ul>
 */
@Component
public class OutboxRelay implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final RedisScript<Long> LOCK =
            RedisScript.of(new ClassPathResource("scripts/lock_acquire_or_extend.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxProperties properties;
    private final FmsMetrics metrics;
    private final String instanceId = "relay-" + UUID.randomUUID();

    public OutboxRelay(StringRedisTemplate redis, KafkaTemplate<String, String> kafkaTemplate,
                       ObjectMapper objectMapper, OutboxProperties properties, FmsMetrics metrics) {
        this.redis = redis;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(this::relaySafely, properties.relayInterval());
    }

    private void relaySafely() {
        try {
            relayOnce();
        } catch (RuntimeException e) {
            log.warn("아웃박스 릴레이 실패, 다음 주기에 재시도합니다: {}", e.getMessage());
        }
    }

    /** 한 묶음을 발행하고, 발행 확인된 건수를 돌려준다. */
    public int relayOnce() {
        Long locked = redis.execute(LOCK, List.of(RedisKeys.OUTBOX_RELAY_LOCK),
                instanceId, String.valueOf(properties.lockTtl().toMillis()));
        if (locked == null || locked != 1L) {
            return 0;
        }
        List<String> entries = redis.opsForList().range(RedisKeys.OUTBOX, 0, properties.batchSize() - 1L);
        if (entries == null || entries.isEmpty()) {
            return 0;
        }

        List<CompletableFuture<?>> sends = new ArrayList<>(entries.size());
        for (String entry : entries) {
            try {
                OutboxMessage message = objectMapper.readValue(entry, OutboxMessage.class);
                sends.add(kafkaTemplate.send(message.topic(), message.key(),
                        objectMapper.writeValueAsString(message.payload())));
            } catch (JsonProcessingException e) {
                // 해석할 수 없는 항목이 큐를 영원히 막지 않도록 따로 빼 두고 지나간다.
                log.error("해석할 수 없는 아웃박스 항목을 {} 로 옮깁니다: {}", RedisKeys.OUTBOX_DEAD, entry, e);
                redis.opsForList().rightPush(RedisKeys.OUTBOX_DEAD, entry);
                sends.add(CompletableFuture.completedFuture(null));
            } catch (RuntimeException e) {
                // send 자체가 실패(메타데이터 조회 시간 초과 등)하면 여기서 멈추고 앞선 것만 확인한다.
                log.warn("아웃박스 발행 중단: {}", e.getMessage());
                break;
            }
        }

        long deadline = System.nanoTime() + properties.sendTimeout().toNanos();
        int confirmed = 0;
        for (CompletableFuture<?> send : sends) {
            try {
                send.get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                confirmed++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("아웃박스 발행 확인 실패, 이후 항목은 다음 주기에 다시 보냅니다: {}", e.getMessage());
                break;
            }
        }
        if (confirmed > 0) {
            // 릴레이만 앞에서 지우고, 새 항목은 뒤에 붙으므로 LTRIM 이 새 항목을 지우지 않는다.
            redis.opsForList().trim(RedisKeys.OUTBOX, confirmed, -1);
            metrics.outboxPublished(confirmed);
        }
        return confirmed;
    }
}
