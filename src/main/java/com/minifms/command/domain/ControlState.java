package com.minifms.command.domain;

/**
 * 차량 제어 상태(디지털 트윈의 desired 값). 명령 요청 시 목표 값으로 먼저 바뀌고(낙관적 반영),
 * 실패·타임아웃이면 이전 값으로 되돌아간다.
 */
public record ControlState(String door, String engine) {
}
