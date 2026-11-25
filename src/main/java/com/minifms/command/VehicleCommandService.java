package com.minifms.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.command.VehicleCommandRepository.CreateResult;
import com.minifms.command.VehicleCommandRepository.TransitionResult;
import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.CommandType;
import com.minifms.command.domain.VehicleCommand;
import com.minifms.command.messaging.AckResult;
import com.minifms.command.messaging.CommandCompensationEvent;
import com.minifms.command.messaging.VehicleCommandAck;
import com.minifms.command.messaging.VehicleCommandMessage;
import com.minifms.common.config.properties.CommandProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.kafka.EventPublishException;
import com.minifms.common.kafka.KafkaEventPublisher;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.outbox.OutboxMessage;
import com.minifms.common.web.ConflictException;
import com.minifms.common.web.NotFoundException;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 원격 제어 명령 사가(오케스트레이션).
 *
 * <pre>
 * 1. 생성   : 잠금·이전 값 스냅샷·목표 값 선반영·마감 등록 (Redis Lua, 원자적)
 * 2. 전송   : vehicle-commands 발행, 브로커 확인 → TRANSMITTED
 * 3. 응답   : vehicle-command-acks 의 SUCCESS → ACKNOWLEDGED
 * 보상      : 전송 실패·차량 거절·마감 초과 → FAILED/TIMEOUT
 *             + 제어 상태 롤백 + 잠금 해제 + vehicle-command-compensations 이벤트(아웃박스) — 한 Lua 안에서
 * </pre>
 * 상태 전이는 compare-and-set 이라, ACK 와 타임아웃이 경합해도 먼저 반영된 쪽 하나만 이긴다.
 */
@Service
public class VehicleCommandService {

    private static final Logger log = LoggerFactory.getLogger(VehicleCommandService.class);

    private final VehicleCommandRepository repository;
    private final VehicleLocationRepository locationRepository;
    private final KafkaEventPublisher publisher;
    private final ObjectMapper objectMapper;
    private final TopicProperties topics;
    private final CommandProperties properties;
    private final FmsMetrics metrics;
    private final Clock clock;

    public VehicleCommandService(VehicleCommandRepository repository, VehicleLocationRepository locationRepository,
                                 KafkaEventPublisher publisher, ObjectMapper objectMapper, TopicProperties topics,
                                 CommandProperties properties, FmsMetrics metrics, Clock clock) {
        this.repository = repository;
        this.locationRepository = locationRepository;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.topics = topics;
        this.properties = properties;
        this.metrics = metrics;
        this.clock = clock;
    }

    public VehicleCommand request(String vehicleId, CommandType type) {
        if (!locationRepository.exists(vehicleId)) {
            throw new NotFoundException("텔레메트리를 보낸 적 없는 차량입니다: " + vehicleId);
        }
        String commandId = UUID.randomUUID().toString();
        long now = clock.millis();
        long deadline = now + properties.ackTimeout().toMillis();

        // 1) 로컬 트랜잭션: 잠금 + 스냅샷 + 선반영 + 마감 등록
        CreateResult created = repository.create(commandId, vehicleId, type, now, deadline,
                properties.ackTimeout().plus(properties.lockGrace()), properties.recordRetention());
        if (!created.created()) {
            throw new ConflictException("이 차량에 처리 중인 명령이 있습니다. 끝난 뒤 다시 요청하세요.",
                    Map.of("vehicleId", vehicleId, "inFlightCommandId", created.detail()));
        }

        // 2) 전송: 브로커가 받았는지(acks=all)까지 확인한다. 차량 응답은 비동기로 온다.
        //    차량은 서버 마감보다 deviceExecutionMargin 만큼 이른 expiresAt 까지만 실행한다(ACK 가 서버 마감 전에 도착할 여유).
        try {
            publisher.publishAndWait(topics.commands(), vehicleId,
                    new VehicleCommandMessage(commandId, vehicleId, type, now, properties.deviceExpiresAt(now)),
                    properties.transmitTimeout());
            repository.transition(commandId, vehicleId, CommandStatus.TRANSMITTED, EnumSet.of(CommandStatus.REQUESTED),
                    clock.millis(), false, false, null, false, null);
        } catch (EventPublishException e) {
            if (e.outcomeUnknown()) {
                // 브로커가 받았는지 모른다(확인 시간 초과 등). 프로듀서는 계속 재전송하므로 이미 차량에 전달됐을 수 있다.
                // 실패로 확정해 보상하지 않고 진행 중(REQUESTED)으로 둔다. ACK 가 오면 ACKNOWLEDGED,
                // 마감까지 오지 않으면 타임아웃 스케줄러가 TIMEOUT 으로 보상한다.
                metrics.commandTransmitUnknown();
                log.warn("명령 전송 결과를 알 수 없어 응답을 기다립니다 commandId={} vehicleId={}: {}",
                        commandId, vehicleId, e.getMessage());
            } else {
                // 브로커에 기록되지 않은 것이 확실한 실패(직렬화·크기·권한 오류 등)만 바로 보상한다.
                log.warn("명령 전송 실패 commandId={} vehicleId={}: {}", commandId, vehicleId, e.getMessage());
                repository.find(commandId).ifPresent(command -> compensate(command, CommandStatus.FAILED, "TRANSMIT_FAILED", false));
            }
        }
        return repository.find(commandId).orElseThrow(() -> new IllegalStateException("방금 만든 명령이 없습니다: " + commandId));
    }

    public VehicleCommand get(String vehicleId, String commandId) {
        return repository.find(commandId)
                .filter(command -> command.vehicleId().equals(vehicleId))
                .orElseThrow(() -> new NotFoundException("명령을 찾을 수 없습니다: " + commandId));
    }

    /** 3) 차량 응답 처리. 같은 응답이 여러 번 와도 상태 전이는 한 번만 일어난다. */
    public void handleAck(VehicleCommandAck ack) {
        Optional<VehicleCommand> found = repository.find(ack.commandId());
        if (found.isEmpty()) {
            log.warn("알 수 없는(또는 보존 기간이 지난) 명령의 응답을 무시합니다 commandId={}", ack.commandId());
            return;
        }
        VehicleCommand command = found.get();
        if (!command.vehicleId().equals(ack.vehicleId())) {
            log.warn("명령과 응답의 차량이 다릅니다 commandId={} expected={} actual={}",
                    command.commandId(), command.vehicleId(), ack.vehicleId());
            return;
        }

        if (ack.result() == AckResult.SUCCESS) {
            TransitionResult result = repository.transition(command.commandId(), command.vehicleId(),
                    CommandStatus.ACKNOWLEDGED, CommandStatus.PENDING, clock.millis(), true, false, null, false, null);
            if (result.applied()) {
                metrics.commandFinished(command.type().name(), CommandStatus.ACKNOWLEDGED.name());
                log.info("명령 완료 commandId={} vehicleId={} type={}", command.commandId(), command.vehicleId(), command.type());
            } else if (result.status() == CommandStatus.TIMEOUT || result.status() == CommandStatus.FAILED) {
                // 차량이 expiresAt 전에 실행했어도 ACK 가 늦게(전달 지연·소비 지연) 도착하면 생긴다. 서버는 이미 기록해 둔
                // 제어 상태를 이전 값으로 되돌렸으므로, 실제 차량 상태와 서버 기록이 다를 수 있다.
                // 자동 보정은 하지 않고(후속 과제) 지표·로그로 남겨 실제 차량 상태를 확인하게 한다.
                metrics.lateAck();
                log.warn("이미 {} 로 보상 처리된 명령에 성공 응답이 늦게 왔습니다. 서버 기록({}={})과 실제 차량 상태({})가 다를 수 있어 "
                                + "확인이 필요합니다 commandId={} vehicleId={}",
                        result.status(), command.type().field().hashField(), command.previousValue(),
                        command.targetValue(), command.commandId(), command.vehicleId());
            }
        } else {
            compensate(command, CommandStatus.FAILED, "REJECTED_BY_VEHICLE: " + ack.reason(), false);
        }
    }

    /** 마감이 지난 명령을 TIMEOUT 으로 보상한다. 보상한 건수를 돌려준다. */
    public int expireOverdue() {
        List<String> overdue = repository.findOverdueIds(clock.millis(), properties.timeoutScanBatchSize());
        int expired = 0;
        for (String commandId : overdue) {
            // 한 항목의 오류(손상된 기록 등)가 나머지 명령의 타임아웃 보상을 막지 않게 항목마다 따로 처리한다.
            try {
                Optional<VehicleCommand> command = repository.find(commandId);
                if (command.isEmpty()) {
                    repository.removeDeadline(commandId); // 보존 기간이 지나 기록이 사라진 경우
                    continue;
                }
                String reason = "NO_ACK_WITHIN_" + properties.ackTimeout().toMillis() + "MS";
                if (compensate(command.get(), CommandStatus.TIMEOUT, reason, true)) {
                    expired++;
                } else if (command.get().status().isTerminal()) {
                    repository.removeDeadline(commandId);
                }
            } catch (RuntimeException e) {
                log.error("명령 타임아웃 처리 실패, 다음 주기에 다시 시도합니다 commandId={}: {}", commandId, e.toString());
            }
        }
        return expired;
    }

    /** 보상 트랜잭션: 실패 상태로 전이 + 제어 상태 롤백 + 잠금 해제 + 보상 이벤트 적재(원자적). */
    private boolean compensate(VehicleCommand command, CommandStatus failure, String reason, boolean requireDeadlinePassed) {
        CommandCompensationEvent event = new CommandCompensationEvent(
                UUID.randomUUID().toString(),
                command.commandId(),
                command.vehicleId(),
                command.type(),
                failure,
                reason,
                command.type().field().hashField(),
                command.previousValue(),
                command.targetValue(),
                Instant.now(clock));
        String outboxEntry = OutboxMessage.serialize(objectMapper, topics.commandCompensations(), command.vehicleId(), event);
        TransitionResult result = repository.transition(command.commandId(), command.vehicleId(), failure,
                CommandStatus.PENDING, clock.millis(), true, true, reason, requireDeadlinePassed, outboxEntry);
        if (result.applied()) {
            metrics.commandFinished(command.type().name(), failure.name());
            log.warn("명령 보상 처리 commandId={} vehicleId={} type={} status={} reason={} {}: {} → {}",
                    command.commandId(), command.vehicleId(), command.type(), failure, reason,
                    command.type().field().hashField(), command.targetValue(), command.previousValue());
        }
        return result.applied();
    }
}
