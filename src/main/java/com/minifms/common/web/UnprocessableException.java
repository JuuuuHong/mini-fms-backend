package com.minifms.common.web;

import java.util.Map;

/** 요청 형식은 맞지만 업무 규칙상 처리할 수 없음(422). 예: 반납할 수 없는 구역. */
public class UnprocessableException extends RuntimeException {

    private final Map<String, Object> properties;

    public UnprocessableException(String message, Map<String, Object> properties) {
        super(message);
        this.properties = Map.copyOf(properties);
    }

    public Map<String, Object> properties() {
        return properties;
    }
}
