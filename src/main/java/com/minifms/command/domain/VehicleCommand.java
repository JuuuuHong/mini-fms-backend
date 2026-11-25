package com.minifms.command.domain;

import java.time.Instant;
import java.util.Map;

/** Redis 명령 해시를 읽은 값. */
public record VehicleCommand(
        String commandId,
        String vehicleId,
        CommandType type,
        CommandStatus status,
        String previousValue,
        String targetValue,
        Instant requestedAt,
        Instant transmittedAt,
        Instant completedAt,
        Instant deadlineAt,
        String failureReason,
        boolean rolledBack) {

    public static VehicleCommand fromHash(Map<String, String> hash) {
        return new VehicleCommand(
                hash.get("commandId"),
                hash.get("vehicleId"),
                CommandType.valueOf(hash.get("type")),
                CommandStatus.valueOf(hash.get("status")),
                hash.get("previousValue"),
                hash.get("targetValue"),
                instant(hash.get("requestedAt")),
                instant(hash.get("transmittedAt")),
                instant(hash.get("completedAt")),
                instant(hash.get("deadlineAt")),
                hash.get("failureReason"),
                "1".equals(hash.get("rolledBack")));
    }

    private static Instant instant(String epochMillis) {
        return epochMillis == null ? null : Instant.ofEpochMilli(Long.parseLong(epochMillis));
    }
}
