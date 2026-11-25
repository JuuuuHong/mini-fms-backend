package com.minifms.incident;

/** 노변 기지국(RSU)·도로 센서가 보고하는 돌발상황 종류. */
public enum IncidentType {
    ROAD_DEBRIS("도로 낙하물"),
    ACCIDENT("교통사고"),
    HARD_BRAKING("급정거 차량");

    private final String label;

    IncidentType(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
