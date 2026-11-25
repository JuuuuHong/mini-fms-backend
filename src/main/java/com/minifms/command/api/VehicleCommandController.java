package com.minifms.command.api;

import com.minifms.command.VehicleCommandService;
import com.minifms.command.domain.CommandType;
import com.minifms.command.domain.VehicleCommand;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 원격 제어 API. 명령은 비동기로 처리되므로 202 와 함께 현재 상태를 돌려주고,
 * 클라이언트는 Location 의 조회 API 로 최종 상태(ACKNOWLEDGED/TIMEOUT/FAILED)를 확인한다.
 */
@RestController
@RequestMapping("/api/v1/vehicles/{vehicleId}/commands")
public class VehicleCommandController {

    private static final String VEHICLE_ID_PATTERN = "^[A-Za-z0-9_-]{1,64}$";

    private final VehicleCommandService commandService;

    public VehicleCommandController(VehicleCommandService commandService) {
        this.commandService = commandService;
    }

    @PostMapping
    public ResponseEntity<VehicleCommand> request(
            @PathVariable @Pattern(regexp = VEHICLE_ID_PATTERN) String vehicleId,
            @Valid @RequestBody CommandRequest request) {
        VehicleCommand command = commandService.request(vehicleId, request.type());
        URI location = URI.create("/api/v1/vehicles/" + vehicleId + "/commands/" + command.commandId());
        return ResponseEntity.accepted().location(location).body(command);
    }

    @GetMapping("/{commandId}")
    public VehicleCommand get(
            @PathVariable @Pattern(regexp = VEHICLE_ID_PATTERN) String vehicleId,
            @PathVariable @Pattern(regexp = "^[0-9a-fA-F-]{36}$") String commandId) {
        return commandService.get(vehicleId, commandId);
    }

    public record CommandRequest(@NotNull CommandType type) {
    }
}
