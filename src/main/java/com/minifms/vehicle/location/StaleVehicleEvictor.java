package com.minifms.vehicle.location;

import com.minifms.common.config.properties.LocationProperties;
import java.time.Clock;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;

/**
 * 오래 수신이 없는 차량(시동 꺼짐·통신 두절)을 GEO 인덱스에서 정리한다.
 * 반경 조회는 활성 여부를 따로 거르므로 정확성에는 영향이 없고, 인덱스 크기를 일정하게 유지하는 용도다.
 */
@Component
public class StaleVehicleEvictor implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(StaleVehicleEvictor.class);
    private static final int MAX_PER_RUN = 1_000;

    private final VehicleLocationRepository repository;
    private final LocationProperties properties;
    private final Clock clock;

    public StaleVehicleEvictor(VehicleLocationRepository repository, LocationProperties properties, Clock clock) {
        this.repository = repository;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(this::evict, properties.evictionInterval());
    }

    void evict() {
        try {
            long evicted = repository.evictStale(Instant.now(clock).minus(properties.staleAfter()), MAX_PER_RUN);
            if (evicted > 0) {
                log.info("수신이 끊긴 차량 {}대를 위치 인덱스에서 정리했습니다(기준 {})", evicted, properties.staleAfter());
            }
        } catch (RuntimeException e) {
            log.warn("오래된 차량 정리 실패: {}", e.getMessage());
        }
    }
}
