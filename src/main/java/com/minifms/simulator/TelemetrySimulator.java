package com.minifms.simulator;

import com.minifms.common.config.properties.SimulatorProperties;
import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.telematics.ingest.TelematicsIngestService;
import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

/**
 * 가상 차량 N대가 주기(기본 1초)마다 텔레메트리를 telematics-raw-events 로 보낸다.
 * duplicateRate 확률로 같은 메시지를 한 번 더 보내 재전송 상황을 만든다.
 */
@Component
@ConditionalOnProperty(prefix = "fms.simulator.telemetry", name = "enabled", havingValue = "true")
public class TelemetrySimulator implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(TelemetrySimulator.class);
    private static final long SUMMARY_EVERY_TICKS = 30;

    private final SimulatedFleet fleet;
    private final TelematicsIngestService ingestService;
    private final SimulatorProperties.Telemetry properties;
    private final Clock clock;
    private final AtomicLong ticks = new AtomicLong();
    private final AtomicLong duplicatesSent = new AtomicLong();
    private long lastTickMillis;

    public TelemetrySimulator(SimulatedFleet fleet, TelematicsIngestService ingestService,
                              SimulatorProperties properties, Clock clock) {
        this.fleet = fleet;
        this.ingestService = ingestService;
        this.properties = properties.telemetry();
        this.clock = clock;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        log.info("텔레메트리 시뮬레이터 시작: 차량 {}대, 주기 {}, 중복 전송 확률 {}",
                properties.vehicleCount(), properties.interval(), properties.duplicateRate());
        registrar.addFixedRateTask(this::tick, properties.interval());
    }

    void tick() {
        try {
            long now = clock.millis();
            double elapsedSeconds = lastTickMillis == 0
                    ? properties.interval().toMillis() / 1000.0
                    : (now - lastTickMillis) / 1000.0;
            lastTickMillis = now;
            for (SimulatedVehicle vehicle : fleet.vehicles()) {
                TelematicsEvent event = vehicle.advance(elapsedSeconds, now, properties.speedMultiplier());
                ingestService.publishAsync(event);
                if (ThreadLocalRandom.current().nextDouble() < properties.duplicateRate()) {
                    ingestService.publishAsync(event);
                    duplicatesSent.incrementAndGet();
                }
            }
            if (ticks.incrementAndGet() % SUMMARY_EVERY_TICKS == 0) {
                log.info("시뮬레이터: {}회 발행, 의도적 중복 {}건 누적", ticks.get(), duplicatesSent.get());
            }
        } catch (RuntimeException e) {
            log.warn("시뮬레이터 발행 실패: {}", e.getMessage());
        }
    }
}
