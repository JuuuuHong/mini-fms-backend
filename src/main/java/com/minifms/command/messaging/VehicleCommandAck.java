package com.minifms.command.messaging;

/** vehicle-command-acks 토픽 메시지(차량 단말 → 서버). */
public record VehicleCommandAck(
        String commandId,
        String vehicleId,
        AckResult result,
        String reason,
        long respondedAt) {
}
