package com.minifms.command;

import com.minifms.common.config.properties.CommandProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

/**
 * 응답 마감 감시. 마감은 Redis sorted set 에 있어 인스턴스가 여러 대여도 같은 목록을 보고,
 * 전이가 compare-and-set 이라 한 명령은 한 인스턴스만 보상한다(별도 잠금 불필요).
 */
@Component
public class CommandTimeoutScheduler implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(CommandTimeoutScheduler.class);

    private final VehicleCommandService commandService;
    private final CommandProperties properties;

    public CommandTimeoutScheduler(VehicleCommandService commandService, CommandProperties properties) {
        this.commandService = commandService;
        this.properties = properties;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(this::scan, properties.timeoutScanInterval());
    }

    private void scan() {
        try {
            int expired = commandService.expireOverdue();
            if (expired > 0) {
                log.info("응답 마감을 넘긴 명령 {}건을 TIMEOUT 으로 보상했습니다", expired);
            }
        } catch (RuntimeException e) {
            log.warn("명령 타임아웃 검사 실패: {}", e.getMessage());
        }
    }
}
