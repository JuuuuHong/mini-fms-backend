package com.minifms.command.domain;

import java.util.EnumSet;
import java.util.Set;

/**
 * 명령 상태 머신.
 * <pre>
 *   REQUESTED ──전송 확인──▶ TRANSMITTED ──차량 ACK──▶ ACKNOWLEDGED
 *       │                       │
 *       │                       ├──차량 거절(NACK)──▶ FAILED   (롤백 + 보상 이벤트)
 *       │                       └──마감 초과───────▶ TIMEOUT  (롤백 + 보상 이벤트)
 *       ├──전송 실패────────────────────────────────▶ FAILED   (롤백 + 보상 이벤트)
 *       └──(전송 확인 전 ACK·마감 초과도 같은 규칙)
 * </pre>
 */
public enum CommandStatus {
    REQUESTED,
    TRANSMITTED,
    ACKNOWLEDGED,
    TIMEOUT,
    FAILED;

    /** 차량 응답을 기다리는 상태. */
    public static final Set<CommandStatus> PENDING = EnumSet.of(REQUESTED, TRANSMITTED);

    public boolean isTerminal() {
        return !PENDING.contains(this);
    }
}
