package com.minifms.support;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

import com.minifms.security.Scopes;
import java.util.Arrays;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.JwtRequestPostProcessor;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * 통합 테스트 공통 기반. Kafka·Redis·ClickHouse 컨테이너를 JVM 당 한 번 띄우고(싱글턴),
 * 모든 테스트 클래스가 같은 Spring 컨텍스트를 재사용한다. 테스트끼리 상태를 공유하므로
 * 각 테스트는 고유한 vehicleId·좌표를 쓴다.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
public abstract class IntegrationTestSupport {

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
    protected static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);
    protected static final ClickHouseContainer CLICKHOUSE =
            new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.8"))
                    .withDatabaseName("fms");

    static {
        Startables.deepStart(KAFKA, REDIS, CLICKHOUSE).join();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.datasource.url", CLICKHOUSE::getJdbcUrl);
        registry.add("spring.datasource.username", CLICKHOUSE::getUsername);
        registry.add("spring.datasource.password", CLICKHOUSE::getPassword);
    }

    @Autowired
    protected MockMvc mockMvc;

    /** 서명 검증을 건너뛰고 주어진 scope 의 인증을 붙인다(인가 규칙만 시험). 실제 토큰 검증은 SecurityIT. */
    protected static JwtRequestPostProcessor withScopes(String... scopes) {
        return jwt().authorities(Arrays.stream(scopes)
                .map(scope -> (GrantedAuthority) new SimpleGrantedAuthority(Scopes.authority(scope)))
                .toList());
    }

    protected static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
