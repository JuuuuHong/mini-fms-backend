package com.minifms.ride.device;

import com.minifms.command.VehicleCommandRepository;
import com.minifms.command.VehicleCommandService;
import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.CommandType;
import com.minifms.command.domain.VehicleCommand;
import com.minifms.common.config.properties.RideProperties;
import com.minifms.common.lock.DistributedLock;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.web.ConflictException;
import com.minifms.common.web.NotFoundException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

/**
 * 반납된 기기의 원격 잠금(LOCKED → AVAILABLE).
 *
 * <p>반납 트랜잭션은 기기를 LOCKED 로 바꾸고 lock_retry_at = 지금으로 둔다. 이 조정기가 주기마다 차례가 된 LOCKED 기기를 보고:
 * <pre>
 *   잠금 명령 없음          → 명령 ID 로 기기 행 선점 → DOOR_LOCK 요청(기존 명령 사가: 선반영·ACK·타임아웃·보상)
 *   명령 ACKNOWLEDGED       → AVAILABLE (다시 대여 가능)
 *   명령 FAILED·TIMEOUT·만료 → 명령을 비우고 지수 백오프 뒤 다시 보냄(기기는 LOCKED 유지 — 잠김이 확인되지 않은 기기는 빌려주지 않는다)
 *   명령 진행 중            → 잠시 뒤 다시 확인
 * </pre>
 * 할 일을 DB 의 기기 상태로 들고 있어 반납 API 가 명령 요청 전에 죽어도, 명령 저장소(Redis)가 잠시 멈춰도 다음 주기에 이어서 한다.
 * 한 인스턴스만 돌도록 분산 락을 쓰지만, 전이는 모두 조건부 갱신이라 겹쳐 돌아도 상태가 어긋나지 않는다(잠금 명령이 한 번 더 갈 뿐이다).
 */
@Component
public class DeviceLockReconciler implements SchedulingConfigurer {

    static final String LOCK_KEY = "fms:lock:device-lock-reconciler";
    private static final Logger log = LoggerFactory.getLogger(DeviceLockReconciler.class);

    private final DeviceRepository devices;
    private final VehicleCommandService commandService;
    private final VehicleCommandRepository commandRepository;
    private final DistributedLock lock;
    private final RideProperties.LockReconcile properties;
    private final FmsMetrics metrics;
    private final Clock clock;

    public DeviceLockReconciler(DeviceRepository devices, VehicleCommandService commandService,
                                VehicleCommandRepository commandRepository, DistributedLock lock,
                                RideProperties properties, FmsMetrics metrics, Clock clock) {
        this.devices = devices;
        this.commandService = commandService;
        this.commandRepository = commandRepository;
        this.lock = lock;
        this.properties = properties.lockReconcile();
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(this::reconcileSafely, properties.interval());
    }

    private void reconcileSafely() {
        try {
            lock.tryExecute(LOCK_KEY, this::reconcileOnce);
        } catch (RuntimeException e) {
            log.warn("기기 잠금 조정 실패, 다음 주기에 재시도합니다: {}", e.toString());
        }
    }

    /** 차례가 된 LOCKED 기기를 한 번씩 처리하고, AVAILABLE 로 돌린 수를 돌려준다. */
    public int reconcileOnce() {
        List<Device> due = devices.findLockDue(clock.instant(), properties.batchSize());
        int released = 0;
        for (Device device : due) {
            // 한 기기의 오류가 나머지 기기의 처리를 막지 않게 기기마다 따로 처리한다.
            try {
                Outcome outcome = reconcile(device);
                if (outcome == Outcome.RELEASED) {
                    released++;
                } else if (outcome == Outcome.BROKER_UNCERTAIN) {
                    // 명령 전송 확인이 오지 않았다(브로커 장애 추정). 남은 기기마다 전송 대기(최대 transmit-timeout)로
                    // 스케줄러 스레드를 오래 붙잡지 않도록 이번 주기를 멈춘다. 남은 기기는 다음 주기에 본다.
                    log.warn("잠금 명령 전송 확인이 없어 이번 조정 주기를 멈춥니다 deviceId={}", device.deviceId());
                    break;
                }
            } catch (RuntimeException e) {
                log.warn("기기 잠금 조정 실패 deviceId={}: {}", device.deviceId(), e.toString());
                devices.postponeLockCheck(device.deviceId(), device.lockCommandId(),
                        clock.instant().plus(properties.backoff(Math.max(1, device.lockAttempts()))));
            }
        }
        return released;
    }

    enum Outcome { RELEASED, WAITING, BROKER_UNCERTAIN }

    private Outcome reconcile(Device device) {
        if (device.lockCommandId() == null) {
            return requestLock(device);
        }
        Instant now = clock.instant();
        Optional<VehicleCommand> command = commandRepository.find(device.lockCommandId());
        CommandStatus status = command.map(VehicleCommand::status).orElse(null);
        if (status == CommandStatus.ACKNOWLEDGED) {
            boolean released = devices.markAvailable(device.deviceId(), device.lockCommandId(), now);
            if (released) {
                metrics.deviceLock("CONFIRMED");
                log.info("잠금 확인, 대여 가능 deviceId={} commandId={} attempts={}",
                        device.deviceId(), device.lockCommandId(), device.lockAttempts());
            }
            return released ? Outcome.RELEASED : Outcome.WAITING;
        }
        if (status == null || status.isTerminal()) {
            // FAILED·TIMEOUT, 또는 기록 보존 기간이 지나 사라진 명령: 잠김이 확인되지 않았으므로 다시 보낸다.
            Instant retryAt = now.plus(properties.backoff(device.lockAttempts()));
            devices.clearFailedLockCommand(device.deviceId(), device.lockCommandId(), retryAt);
            metrics.deviceLock("FAILED");
            log.warn("잠금 명령 실패 deviceId={} commandId={} status={} reason={} attempts={}, {} 에 다시 보냅니다",
                    device.deviceId(), device.lockCommandId(), status,
                    command.map(VehicleCommand::failureReason).orElse("기록 없음"), device.lockAttempts(), retryAt);
            return Outcome.WAITING;
        }
        devices.postponeLockCheck(device.deviceId(), device.lockCommandId(), now.plus(properties.pendingCheckInterval()));
        return Outcome.WAITING;
    }

    /**
     * 선점 후 발송: 명령 ID 를 먼저 기기 행에 기록하고(LOCKED·진행 중 명령 없음일 때만), 그다음 명령을 보낸다.
     * 이 목록은 주기 시작 때 읽은 스냅샷이라, 그사이 기기가 잠금 확인 → 대여(IN_USE)로 바뀌었을 수 있다. 선점이 실패하면 보내지 않아
     * 주행 중인 기기를 잠그지 않는다. 선점 뒤 발송 전에 죽으면 명령 기록이 없으므로 다음 확인 때 실패로 보고 다시 보낸다.
     */
    private Outcome requestLock(Device device) {
        Instant now = clock.instant();
        String commandId = UUID.randomUUID().toString();
        if (!devices.claimLockCommand(device.deviceId(), commandId, now.plus(properties.pendingCheckInterval()))) {
            return Outcome.WAITING;
        }
        VehicleCommand command;
        try {
            command = commandService.request(device.deviceId(), CommandType.DOOR_LOCK, commandId);
        } catch (ConflictException e) {
            // 다른 명령(관제의 ENGINE_STOP 등)이 진행 중이다. 그 명령이 끝나면 보낸다.
            devices.releaseLockClaim(device.deviceId(), commandId, now.plus(properties.pendingCheckInterval()));
            return Outcome.WAITING;
        } catch (NotFoundException e) {
            // 텔레메트리를 보낸 적 없는 기기라 명령 대상이 아니다. 단말이 위치를 보내기 시작하면 보낸다.
            devices.releaseLockClaim(device.deviceId(), commandId, now.plus(properties.retryMax()));
            log.warn("텔레메트리가 없는 기기라 잠금 명령을 보내지 못했습니다 deviceId={}", device.deviceId());
            return Outcome.WAITING;
        }
        metrics.deviceLock("REQUESTED");
        log.info("잠금 명령 요청 deviceId={} commandId={} attempt={} status={}", device.deviceId(), commandId,
                device.lockAttempts() + 1, command.status());
        // REQUESTED 로 돌아왔다 = 전송 확인이 마감 안에 오지 않았다(결과 모름). 명령 사가가 ACK·마감으로 마무리한다.
        return command.status() == CommandStatus.REQUESTED ? Outcome.BROKER_UNCERTAIN : Outcome.WAITING;
    }
}
