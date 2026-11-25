package com.minifms.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.OutboxProperties;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.outbox.OutboxRelay;
import com.minifms.common.redis.RedisKeys;
import com.minifms.support.IntegrationTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * 아웃박스 릴레이 정확성. 앱 컨텍스트의 릴레이가 같은 키를 비우지 않도록 Redis DB 1 을 따로 쓰고,
 * Kafka 는 발행 확인 시점을 손으로 정할 수 있는 가짜로 바꾼다.
 */
class OutboxRelayIT extends IntegrationTestSupport {

    static final List<String> ENTRIES = IntStream.rangeClosed(1, 4)
            .mapToObj(i -> "{\"topic\":\"t\",\"key\":\"k\",\"payload\":{\"n\":" + i + "}}")
            .toList();

    @Autowired
    ObjectMapper objectMapper;

    LettuceConnectionFactory connectionFactory;
    StringRedisTemplate redis;
    OutboxProperties properties = new OutboxProperties(Duration.ofMillis(200), 2, Duration.ofSeconds(10), Duration.ofSeconds(10));
    FmsMetrics metrics = new FmsMetrics(new SimpleMeterRegistry());

    @BeforeEach
    void setUp() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(REDIS.getHost(), REDIS.getMappedPort(6379));
        config.setDatabase(1);
        connectionFactory = new LettuceConnectionFactory(config);
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redis = new StringRedisTemplate(connectionFactory);
        redis.delete(List.of(RedisKeys.OUTBOX, RedisKeys.OUTBOX_RELAY_LOCK, RedisKeys.OUTBOX_DEAD));
        redis.opsForList().rightPushAll(RedisKeys.OUTBOX, ENTRIES);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void 발행_확인된_만큼만_앞에서_지운다() {
        OutboxRelay relay = new OutboxRelay(redis, kafkaReturning(CompletableFuture.completedFuture(null)),
                objectMapper, properties, metrics);

        assertThat(relay.relayOnce()).isEqualTo(2);
        assertThat(redis.opsForList().range(RedisKeys.OUTBOX, 0, -1)).containsExactly(ENTRIES.get(2), ENTRIES.get(3));
    }

    @Test
    void 발행을_기다리는_사이_잠금이_만료돼_다른_릴레이가_처리해도_미발행_항목을_지우지_않는다() throws Exception {
        CompletableFuture<SendResult<String, String>> pending = new CompletableFuture<>();
        AtomicInteger slowSends = new AtomicInteger();
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> slowKafka = mock(KafkaTemplate.class);
        when(slowKafka.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            slowSends.incrementAndGet();
            return pending;
        });
        OutboxRelay relayA = new OutboxRelay(redis, slowKafka, objectMapper, properties, metrics);
        OutboxRelay relayB = new OutboxRelay(redis, kafkaReturning(CompletableFuture.completedFuture(null)),
                objectMapper, properties, metrics);

        // A: 1·2번을 읽고 보낸 뒤 브로커 확인을 기다린다.
        CompletableFuture<Integer> runA = CompletableFuture.supplyAsync(relayA::relayOnce);
        await().atMost(Duration.ofSeconds(5)).until(() -> slowSends.get() == 2);

        // 그 사이 A 의 잠금이 만료되고, B 가 같은 1·2번을 발행·삭제한다.
        redis.delete(RedisKeys.OUTBOX_RELAY_LOCK);
        assertThat(relayB.relayOnce()).isEqualTo(2);
        assertThat(redis.opsForList().range(RedisKeys.OUTBOX, 0, -1)).containsExactly(ENTRIES.get(2), ENTRIES.get(3));

        // 이제 A 의 확인이 도착한다. A 는 이미 B 가 지운 항목 대신 아직 발행하지 않은 3·4번을 지우면 안 된다.
        pending.complete(null);
        runA.get(10, TimeUnit.SECONDS);
        assertThat(redis.opsForList().range(RedisKeys.OUTBOX, 0, -1)).containsExactly(ENTRIES.get(2), ENTRIES.get(3));
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafkaReturning(CompletableFuture<SendResult<String, String>> result) {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(result);
        return kafka;
    }
}
