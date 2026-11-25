package com.minifms.command.messaging;

import com.minifms.command.domain.CommandType;

/**
 * vehicle-commands 토픽 메시지(서버 → 차량 단말).
 *
 * @param expiresAt 이 시각(epoch ms)이 지나면 단말은 명령을 실행하지 않는다. 서버가 이미 TIMEOUT 으로
 *                  롤백한 뒤에 늦게 도착한 명령이 실행돼 상태가 어긋나는 것을 막는다.
 */
public record VehicleCommandMessage(
        String commandId,
        String vehicleId,
        CommandType type,
        long issuedAt,
        long expiresAt) {
}
