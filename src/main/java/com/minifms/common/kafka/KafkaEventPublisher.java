package com.minifms.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * 도메인 객체를 JSON 으로 바꿔 Kafka 에 싣는다. 키는 항상 vehicleId(돌발상황 결과는 incidentId)로 주어
 * 같은 대상의 이벤트가 같은 파티션에서 순서를 지키게 한다.
 */
@Component
public class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 발행하고 브로커 확인을 기다리는 future 를 돌려준다. 예외를 던지지 않는다.
     *
     * <p>KafkaTemplate 은 즉시 실패(브로커 메타데이터를 max.block.ms 안에 얻지 못한 경우 등)를 호출 스레드에
     * 예외로 던진다. 호출하는 쪽이 성공·실패를 한 경로(future)로 다루도록 실패한 future 로 바꿔 돌려준다.
     */
    public CompletableFuture<SendResult<String, String>> publish(String topic, String key, Object payload) {
        String json = toJson(payload);
        try {
            return kafkaTemplate.send(topic, key, json);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
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
            throw new EventPublishException("Kafka 발행 대기 중 인터럽트", e, true);
        } catch (ExecutionException e) {
            throw new EventPublishException("Kafka 발행 실패", e.getCause(), isOutcomeUnknown(e.getCause()));
        } catch (TimeoutException e) {
            throw new EventPublishException("Kafka 발행 확인 시간 초과(" + timeout + ")", e, true);
        }
    }

    /**
     * 브로커가 레코드를 받았는지 알 수 없는 실패인가.
     * 확인 대기 시간 초과와 재시도 가능한 Kafka 오류(전송 시간 초과·네트워크 오류·복제 부족 등)는 이미 기록됐을 수 있다.
     * 직렬화 오류·레코드 크기 초과·권한 오류 같은 재시도 불가 오류는 기록되지 않은 것이 확실하다.
     */
    public static boolean isOutcomeUnknown(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < 10; depth++) {
            if (current instanceof TimeoutException
                    || current instanceof RetriableException
                    || current instanceof InterruptedException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /** 재시도하면 될 수 있는 Kafka 오류(브로커 장애·시간 초과·네트워크 오류)인가. 원인 사슬을 따라 확인한다. */
    public static boolean isRetriable(Throwable error) {
        Throwable current = error;
        for (int depth = 0; current != null && depth < 10; depth++) {
            if (current instanceof RetriableException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    public String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 직렬화 실패: " + payload.getClass().getSimpleName(), e);
        }
    }
}
