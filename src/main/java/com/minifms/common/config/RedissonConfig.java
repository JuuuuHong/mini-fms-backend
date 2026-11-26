package com.minifms.common.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * 분산 락 전용 Redisson 클라이언트. 연결 정보는 spring.data.redis 를 그대로 쓴다.
 *
 * <p>redisson-spring-boot-starter 는 RedisConnectionFactory 를 Redisson 구현으로 바꿔, 기존 Lettuce 기반 Lua 스크립트·
 * 예외 변환(503 판정)까지 영향을 준다. 그래서 스타터 없이 락에 필요한 클라이언트만 따로 만든다.
 */
@Configuration
public class RedissonConfig {

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(RedisProperties redis) {
        Config config = new Config();
        config.setCodec(StringCodec.INSTANCE);
        // 첫 사용 때 연결한다. Redis 가 잠시 멈춘 사이 재기동해도 앱(수집·돌발상황 경로)은 뜨고, 락을 쓰는 라이드 API 만 503 이다.
        config.setLazyInitialization(true);
        String scheme = redis.getSsl().isEnabled() ? "rediss://" : "redis://";
        SingleServerConfig server = config.useSingleServer()
                .setAddress(scheme + redis.getHost() + ":" + redis.getPort())
                .setDatabase(redis.getDatabase())
                .setConnectionMinimumIdleSize(2)
                .setConnectionPoolSize(16);
        if (StringUtils.hasText(redis.getUsername())) {
            server.setUsername(redis.getUsername());
        }
        if (StringUtils.hasText(redis.getPassword())) {
            server.setPassword(redis.getPassword());
        }
        if (redis.getTimeout() != null) {
            server.setTimeout((int) redis.getTimeout().toMillis());
        }
        if (redis.getConnectTimeout() != null) {
            server.setConnectTimeout((int) redis.getConnectTimeout().toMillis());
        }
        return Redisson.create(config);
    }
}
