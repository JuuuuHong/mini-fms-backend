package com.minifms.common.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Redis 아웃박스 항목. 상태를 바꾸는 Lua 스크립트가 같은 원자 연산 안에서 이 JSON 을 리스트에 넣고,
 * {@link OutboxRelay} 가 꺼내 Kafka 로 발행한다.
 */
public record OutboxMessage(String topic, String key, JsonNode payload) {

    public static String serialize(ObjectMapper mapper, String topic, String key, Object payload) {
        try {
            return mapper.writeValueAsString(new OutboxMessage(topic, key, mapper.valueToTree(payload)));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("아웃박스 직렬화 실패: " + payload.getClass().getSimpleName(), e);
        }
    }
}
