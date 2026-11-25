package com.minifms.command.messaging;

import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.CommandType;
import java.time.Instant;

/**
 * vehicle-command-compensations 토픽 메시지. 명령이 실패·타임아웃으로 끝나 제어 상태를 되돌렸음을 알린다.
 * 구독 예: 고객 앱 푸시("문 잠금 실패"), 관제 화면 갱신, 감사 로그.
 *
 * @param eventId       이벤트 고유 ID(아웃박스는 at-least-once 라 소비자가 이 값으로 중복을 거른다)
 * @param restoredValue 되돌린 값(명령 전 값)
 * @param revertedValue 취소된 목표 값
 */
public record CommandCompensationEvent(
        String eventId,
        String commandId,
        String vehicleId,
        CommandType type,
        CommandStatus failedStatus,
        String reason,
        String field,
        String restoredValue,
        String revertedValue,
        Instant occurredAt) {
}
