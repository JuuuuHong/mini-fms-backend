package com.minifms.common.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClockConfig {

    /** 시간 의존 로직(타임아웃·활성 판정)을 테스트에서 바꿔 끼울 수 있도록 Clock 을 빈으로 둔다. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
