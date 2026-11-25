package com.minifms.command.domain;

public enum CommandType {
    DOOR_LOCK(ControlField.DOOR, "LOCKED"),
    DOOR_UNLOCK(ControlField.DOOR, "UNLOCKED"),
    ENGINE_STOP(ControlField.ENGINE, "OFF");

    private final ControlField field;
    private final String targetValue;

    CommandType(ControlField field, String targetValue) {
        this.field = field;
        this.targetValue = targetValue;
    }

    public ControlField field() {
        return field;
    }

    public String targetValue() {
        return targetValue;
    }
}
