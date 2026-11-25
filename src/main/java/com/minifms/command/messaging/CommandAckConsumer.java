package com.minifms.command.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.command.VehicleCommandService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** 차량 응답(vehicle-command-acks) 소비. */
@Component
public class CommandAckConsumer {

    private static final Logger log = LoggerFactory.getLogger(CommandAckConsumer.class);

    private final VehicleCommandService commandService;
    private final ObjectMapper objectMapper;

    public CommandAckConsumer(VehicleCommandService commandService, ObjectMapper objectMapper) {
        this.commandService = commandService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "command-ack-handler",
            groupId = "${fms.consumers.command-ack-group:fms-command-ack-handler}",
            topics = "${fms.topics.command-acks:vehicle-command-acks}")
    public void onAck(String payload) {
        VehicleCommandAck ack;
        try {
            ack = objectMapper.readValue(payload, VehicleCommandAck.class);
        } catch (JsonProcessingException e) {
            log.warn("해석할 수 없는 명령 응답을 건너뜁니다: {}", e.getOriginalMessage());
            return;
        }
        if (ack.commandId() == null || ack.vehicleId() == null || ack.result() == null) {
            log.warn("필수 값이 빠진 명령 응답을 건너뜁니다: {}", payload);
            return;
        }
        commandService.handleAck(ack);
    }
}
