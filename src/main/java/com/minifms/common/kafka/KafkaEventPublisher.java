package com.minifms.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * 도메인 객체를 JSON 으로 바꿔 Kafka 에 싣는다. 키는 항상 vehicleId 로 주어
 * 같은 차량의 이벤트가 같은 파티션에서 순서를 지키게 한다.
 */
@Component
public class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<String, String>> publish(String topic, String key, Object payload) {
        return kafkaTemplate.send(topic, key, toJson(payload));
    }

    /** 브로커 확인(acks=all)까지 기다린다. 실패하면 {@link EventPublishException}. */
    public void publishAndWait(String topic, String key, Object payload, Duration timeout) {
        awaitAll(List.of(publish(topic, key, payload)), timeout);
    }

    /** 여러 건을 먼저 모두 보내 놓고(배치 전송) 한꺼번에 확인을 기다린다. */
    public void awaitAll(List<CompletableFuture<SendResult<String, String>>> futures, Duration timeout) {
        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("Kafka 발행 대기 중 인터럽트", e);
        } catch (ExecutionException e) {
            throw new EventPublishException("Kafka 발행 실패", e.getCause());
        } catch (TimeoutException e) {
            throw new EventPublishException("Kafka 발행 확인 시간 초과(" + timeout + ")", e);
        }
    }

    public String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 직렬화 실패: " + payload.getClass().getSimpleName(), e);
        }
    }
}
