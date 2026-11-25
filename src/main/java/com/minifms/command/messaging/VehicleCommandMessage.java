package com.minifms.command.messaging;

import com.minifms.command.domain.CommandType;

/**
 * vehicle-commands 토픽 메시지(서버 → 차량 단말).
 *
 * @param expiresAt 이 시각(epoch ms)이 지나면 단말은 명령을 실행하지 않는다. 서버 응답 마감보다
 *                  fms.command.device-execution-margin 만큼 이르다. 서버가 TIMEOUT 으로 보상한 뒤에 늦게 도착한 명령이
 *                  실행되는 일과, 마감 직전에 실행한 명령의 ACK 가 서버 마감을 넘기는 일을 줄인다.
 */
public record VehicleCommandMessage(
        String commandId,
        String vehicleId,
        CommandType type,
        long issuedAt,
        long expiresAt) {
}
