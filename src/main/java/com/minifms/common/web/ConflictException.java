package com.minifms.common.web;

import java.util.Map;

public class ConflictException extends RuntimeException {

    private final Map<String, Object> properties;

    public ConflictException(String message, Map<String, Object> properties) {
        super(message);
        this.properties = Map.copyOf(properties);
    }

    public Map<String, Object> properties() {
        return properties;
    }
}
