package com.minifms.ride.api;

import com.minifms.common.web.NotFoundException;
import com.minifms.ride.device.Device;
import com.minifms.ride.device.DeviceRepository;
import jakarta.validation.constraints.Pattern;
import java.time.Clock;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 대여 기기 등록·조회(관제). 기기 ID 는 텔레매틱스의 vehicleId 와 같다(반납 위치·잠금 명령이 이 ID 로 이어진다).
 * <pre>
 *   PUT /api/v1/devices/{deviceId}   등록(vehicle:control). 새로 만들면 201, 이미 있으면 200 + 현재 상태(멱등)
 *   GET /api/v1/devices/{deviceId}   상태(fleet:read)
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/devices")
public class DeviceController {

    private final DeviceRepository devices;
    private final Clock clock;

    public DeviceController(DeviceRepository devices, Clock clock) {
        this.devices = devices;
        this.clock = clock;
    }

    @PutMapping("/{deviceId}")
    public ResponseEntity<Device> register(@PathVariable @Pattern(regexp = RideController.DEVICE_ID_PATTERN) String deviceId) {
        boolean created = devices.register(deviceId, clock.instant());
        return ResponseEntity.status(created ? HttpStatus.CREATED : HttpStatus.OK).body(find(deviceId));
    }

    @GetMapping("/{deviceId}")
    public Device get(@PathVariable @Pattern(regexp = RideController.DEVICE_ID_PATTERN) String deviceId) {
        return find(deviceId);
    }

    private Device find(String deviceId) {
        return devices.find(deviceId).orElseThrow(() -> new NotFoundException("등록되지 않은 기기입니다: " + deviceId));
    }
}
