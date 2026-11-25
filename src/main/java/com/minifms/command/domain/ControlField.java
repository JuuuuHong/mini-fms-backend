package com.minifms.command.domain;

/** 원격 제어로 바뀌는 차량 상태 항목. defaultValue 는 아직 기록이 없을 때의 값. */
public enum ControlField {
    DOOR("door", "UNLOCKED"),
    ENGINE("engine", "ON");

    private final String hashField;
    private final String defaultValue;

    ControlField(String hashField, String defaultValue) {
        this.hashField = hashField;
        this.defaultValue = defaultValue;
    }

    public String hashField() {
        return hashField;
    }

    public String defaultValue() {
        return defaultValue;
    }
}
