package com.minifms.common.web;

import java.util.List;

public class InvalidRequestException extends RuntimeException {

    private final List<String> violations;

    public InvalidRequestException(String message, List<String> violations) {
        super(message);
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
