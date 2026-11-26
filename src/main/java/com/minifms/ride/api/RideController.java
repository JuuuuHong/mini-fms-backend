package com.minifms.ride.api;

import com.minifms.common.web.InvalidRequestException;
import com.minifms.ride.Ride;
import com.minifms.ride.RideService;
import com.minifms.security.Scopes;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 라이드 API(이용자 앱, ride:write). 이용자는 토큰의 subject 다.
 * <pre>
 *   POST /api/v1/rides                    대여(구독권은 지정한 것만) → 201 + Location
 *   POST /api/v1/rides/{rideId}/return    반납 → 200 (확정 요금). 같은 요청을 다시 보내도 같은 결과(멱등)
 *   GET  /api/v1/rides/{rideId}           본인 라이드(ride:write) 또는 모든 라이드(fleet:read)
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/rides")
public class RideController {

    static final String DEVICE_ID_PATTERN = "^[A-Za-z0-9_-]{1,64}$";

    private final RideService rideService;

    public RideController(RideService rideService) {
        this.rideService = rideService;
    }

    @PostMapping
    public ResponseEntity<Ride> rent(Authentication authentication, @Valid @RequestBody RentRequest request) {
        Ride ride = rideService.rent(userId(authentication), request.deviceId(), request.subscriptionPlan());
        return ResponseEntity.created(URI.create("/api/v1/rides/" + ride.rideId())).body(ride);
    }

    @PostMapping("/{rideId}/return")
    public Ride returnRide(Authentication authentication, @PathVariable UUID rideId) {
        return rideService.returnRide(userId(authentication), rideId);
    }

    @GetMapping("/{rideId}")
    public Ride get(Authentication authentication, @PathVariable UUID rideId) {
        boolean fleetView = authentication.getAuthorities().stream()
                .anyMatch(authority -> Scopes.authority(Scopes.FLEET_READ).equals(authority.getAuthority()));
        return rideService.get(rideId, userId(authentication), fleetView);
    }

    /** 이용자 ID = 토큰 subject. DB 열 길이(64)를 넘거나 비어 있으면 400. */
    private static String userId(Authentication authentication) {
        String subject = authentication.getName();
        if (subject == null || subject.isBlank() || subject.length() > 64) {
            throw new InvalidRequestException("토큰의 subject 가 올바른 이용자 ID 가 아닙니다", List.of("sub: 1~64자"));
        }
        return subject;
    }

    /**
     * @param subscriptionPlan 이번 대여에 쓸 구독권 코드(선택). 지정하지 않으면 정가다. 가진 구독권 중에서 이용자가 고른다.
     */
    public record RentRequest(
            @NotBlank @Pattern(regexp = DEVICE_ID_PATTERN) String deviceId,
            @Pattern(regexp = "^[A-Z0-9_]{1,32}$") String subscriptionPlan) {
    }
}
