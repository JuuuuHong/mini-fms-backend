package com.minifms.simulator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.command.domain.CommandType;
import com.minifms.command.messaging.AckResult;
import com.minifms.command.messaging.VehicleCommandAck;
import com.minifms.command.messaging.VehicleCommandMessage;
import com.minifms.common.config.properties.SimulatorProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.kafka.KafkaEventPublisher;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

/**
 * 가상 차량 단말(TCU). vehicle-commands 를 받아 지연 뒤 vehicle-command-acks 로 응답한다.
 * <ul>
 *   <li>unresponsiveVehicles: 응답하지 않는다 → 서버가 TIMEOUT 보상</li>
 *   <li>rejectingVehicles: 거절(REJECTED) → 서버가 FAILED 보상</li>
 *   <li>그 밖의 차량: 성공(SUCCESS). ENGINE_STOP 이면 가상 차량을 60초 멈춘다.</li>
 * </ul>
 * 실제 단말처럼 expiresAt 이 지난 명령은 실행하지 않는다.
 */
@Component
@ConditionalOnProperty(prefix = "fms.simulator.responder", name = "enabled", havingValue = "true")
public class TcuCommandResponder {

    private static final Logger log = LoggerFactory.getLogger(TcuCommandResponder.class);
    private static final Duration ENGINE_STOP_DURATION = Duration.ofSeconds(60);

    private final KafkaEventPublisher publisher;
    private final TaskScheduler taskScheduler;
    private final SimulatedFleet fleet;
    private final ObjectMapper objectMapper;
    private final TopicProperties topics;
    private final SimulatorProperties.Responder properties;
    private final Set<String> unresponsive;
    private final Set<String> rejecting;
    private final Clock clock;

    public TcuCommandResponder(KafkaEventPublisher publisher, TaskScheduler taskScheduler, SimulatedFleet fleet,
                               ObjectMapper objectMapper, TopicProperties topics, SimulatorProperties properties,
                               Clock clock) {
        this.publisher = publisher;
        this.taskScheduler = taskScheduler;
        this.fleet = fleet;
        this.objectMapper = objectMapper;
        this.topics = topics;
        this.properties = properties.responder();
        this.unresponsive = Set.copyOf(this.properties.unresponsiveVehicles());
        this.rejecting = Set.copyOf(this.properties.rejectingVehicles());
        this.clock = clock;
    }

    @KafkaListener(
            id = "tcu-simulator",
            groupId = "fms-tcu-simulator",
            topics = "${fms.topics.commands:vehicle-commands}")
    public void onCommand(String payload) {
        VehicleCommandMessage command;
        try {
            command = objectMapper.readValue(payload, VehicleCommandMessage.class);
        } catch (JsonProcessingException e) {
            log.warn("[TCU] 해석할 수 없는 명령: {}", e.getOriginalMessage());
            return;
        }
        if (unresponsive.contains(command.vehicleId())) {
            log.info("[TCU] {} 응답 없음(통신 두절 시뮬레이션) commandId={}", command.vehicleId(), command.commandId());
            return;
        }
        long min = properties.minDelay().toMillis();
        long max = Math.max(min + 1, properties.maxDelay().toMillis());
        long delay = ThreadLocalRandom.current().nextLong(min, max);
        taskScheduler.schedule(() -> respond(command), Instant.now(clock).plusMillis(delay));
    }

    private void respond(VehicleCommandMessage command) {
        long now = clock.millis();
        if (now > command.expiresAt()) {
            log.info("[TCU] 유효 시간이 지난 명령은 실행하지 않습니다 commandId={}", command.commandId());
            return;
        }
        VehicleCommandAck ack;
        if (rejecting.contains(command.vehicleId())) {
            ack = new VehicleCommandAck(command.commandId(), command.vehicleId(), AckResult.REJECTED, "VEHICLE_IN_MOTION", now);
        } else {
            if (command.type() == CommandType.ENGINE_STOP) {
                fleet.stopEngine(command.vehicleId(), now + ENGINE_STOP_DURATION.toMillis());
            }
            ack = new VehicleCommandAck(command.commandId(), command.vehicleId(), AckResult.SUCCESS, null, now);
        }
        publisher.publish(topics.commandAcks(), command.vehicleId(), ack)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("[TCU] 응답 발행 실패 commandId={}: {}", command.commandId(), ex.getMessage());
                    }
                });
    }
}
