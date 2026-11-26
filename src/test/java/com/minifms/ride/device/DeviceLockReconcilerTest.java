package com.minifms.ride.device;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.minifms.command.VehicleCommandRepository;
import com.minifms.command.VehicleCommandService;
import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.CommandType;
import com.minifms.command.domain.VehicleCommand;
import com.minifms.common.config.properties.RideProperties;
import com.minifms.common.config.properties.RideProperties.LockReconcile;
import com.minifms.common.config.properties.RideProperties.Pricing;
import com.minifms.common.lock.DistributedLock;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.web.ConflictException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.RedisConnectionFailureException;

/** 잠금 조정기의 발송 규칙(저장소·명령 서비스는 가짜). */
class DeviceLockReconcilerTest {

    static final Instant NOW = Instant.parse("2026-11-26T00:00:00Z");

    final DeviceRepository devices = mock(DeviceRepository.class);
    final VehicleCommandService commandService = mock(VehicleCommandService.class);
    final DeviceLockReconciler reconciler = new DeviceLockReconciler(devices, commandService,
            mock(VehicleCommandRepository.class), mock(DistributedLock.class),
            new RideProperties(Duration.ofSeconds(3), Duration.ofSeconds(30), 3.0, new Pricing(1_000, 150, 500, 10_000, List.of()),
                    new LockReconcile(Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofMinutes(5), 100)),
            new FmsMetrics(new SimpleMeterRegistry()), Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void 기기_행을_선점하지_못하면_잠금_명령을_보내지_않는다() {
        // 주기 시작 때 읽은 스냅샷은 LOCKED 였지만, 그사이 잠금 확인 → 대여(IN_USE)로 바뀌었다.
        when(devices.findLockDue(any(), eq(100))).thenReturn(List.of(locked("KB-1")));
        when(devices.claimLockCommand(eq("KB-1"), anyString(), any())).thenReturn(false);

        reconciler.reconcileOnce();

        verify(commandService, never()).request(anyString(), any(), anyString());
    }

    @Test
    void 선점한_명령_ID_로_보낸다() {
        when(devices.findLockDue(any(), eq(100))).thenReturn(List.of(locked("KB-1")));
        when(devices.claimLockCommand(eq("KB-1"), anyString(), any())).thenReturn(true);
        when(commandService.request(eq("KB-1"), eq(CommandType.DOOR_LOCK), anyString()))
                .thenAnswer(invocation -> command(invocation.getArgument(2), "KB-1", CommandStatus.TRANSMITTED));

        reconciler.reconcileOnce();

        ArgumentCaptor<String> claimed = ArgumentCaptor.forClass(String.class);
        verify(devices).claimLockCommand(eq("KB-1"), claimed.capture(), eq(NOW.plusSeconds(1)));
        verify(commandService).request("KB-1", CommandType.DOOR_LOCK, claimed.getValue());
    }

    @Test
    void 전송_확인이_오지_않으면_남은_기기는_다음_주기로_넘긴다() {
        when(devices.findLockDue(any(), eq(100))).thenReturn(List.of(locked("KB-1"), locked("KB-2"), locked("KB-3")));
        when(devices.claimLockCommand(anyString(), anyString(), any())).thenReturn(true);
        when(commandService.request(anyString(), eq(CommandType.DOOR_LOCK), anyString()))
                .thenAnswer(invocation -> command(invocation.getArgument(2), invocation.getArgument(0), CommandStatus.REQUESTED));

        reconciler.reconcileOnce();

        verify(commandService, times(1)).request(anyString(), any(), anyString());
    }

    @Test
    void 다른_명령이_진행_중이면_선점을_풀고_시도_수를_되돌린다() {
        when(devices.findLockDue(any(), eq(100))).thenReturn(List.of(locked("KB-1")));
        when(devices.claimLockCommand(eq("KB-1"), anyString(), any())).thenReturn(true);
        when(commandService.request(eq("KB-1"), eq(CommandType.DOOR_LOCK), anyString()))
                .thenThrow(new ConflictException("처리 중", Map.of()));

        reconciler.reconcileOnce();

        ArgumentCaptor<String> claimed = ArgumentCaptor.forClass(String.class);
        verify(devices).claimLockCommand(eq("KB-1"), claimed.capture(), any());
        verify(devices).releaseLockClaim("KB-1", claimed.getValue(), NOW.plusSeconds(1));
        assertThat(claimed.getValue()).hasSize(36);
    }

    @Test
    void 선점_뒤_발송이_실패하면_선점을_그대로_두고_선점_때의_확인_시각에_명령_기록으로_판단한다() {
        when(devices.findLockDue(any(), eq(100))).thenReturn(List.of(locked("KB-1")));
        when(devices.claimLockCommand(eq("KB-1"), anyString(), any())).thenReturn(true);
        when(commandService.request(eq("KB-1"), eq(CommandType.DOOR_LOCK), anyString()))
                .thenThrow(new RedisConnectionFailureException("Redis 연결 끊김"));

        reconciler.reconcileOnce();

        // 선점이 확인 시각을 1초 뒤(pendingCheckInterval)로 넣었다. 명령이 나갔을 수 있으므로 선점을 풀지 않고,
        // 백오프로 미루지도 않는다(나가서 ACK 됐다면 1초 뒤 바로 대여 가능이 되어야 한다).
        verify(devices).claimLockCommand(eq("KB-1"), anyString(), eq(NOW.plusSeconds(1)));
        verify(devices, never()).postponeLockCheck(anyString(), any(), any());
        verify(devices, never()).releaseLockClaim(anyString(), anyString(), any());
    }

    private static Device locked(String deviceId) {
        return new Device(deviceId, DeviceState.LOCKED, null, null, 0, NOW, NOW, NOW);
    }

    private static VehicleCommand command(String commandId, String vehicleId, CommandStatus status) {
        return new VehicleCommand(commandId, vehicleId, CommandType.DOOR_LOCK, status, "UNLOCKED", "LOCKED",
                NOW, null, null, NOW.plusSeconds(5), null, false);
    }
}
