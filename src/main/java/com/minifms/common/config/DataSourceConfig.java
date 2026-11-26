package com.minifms.common.config;

import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.jdbc.DataSourceHealthIndicator;
import org.springframework.boot.autoconfigure.flyway.FlywayDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 데이터 소스 두 개.
 * <ul>
 *   <li>ClickHouse(spring.datasource, {@code @Primary}) — 과거 궤적. JdbcTemplate·spring.sql.init 은 이쪽만 쓴다.</li>
 *   <li>PostgreSQL(fms.ride.datasource) — 라이드·기기 상태·결제 아웃박스. Flyway 가 스키마를 만들고, 트랜잭션은 이쪽만 쓴다.</li>
 * </ul>
 * DataSource 빈을 직접 정의하면 Spring Boot 의 DataSource 자동 구성이 물러나므로, ClickHouse 쪽도 자동 구성과 같은 방식
 * (spring.datasource + spring.datasource.hikari)으로 직접 만든다. 라이드 쪽은 JdbcTemplate 빈을 만들지 않는다
 * (만들면 ClickHouse 용 JdbcTemplate 자동 구성이 물러난다). 대신 {@link JdbcClient}·{@link TransactionTemplate} 을 둔다.
 */
@Configuration
public class DataSourceConfig {

    public static final String RIDE_DATA_SOURCE = "rideDataSource";

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource dataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean(RIDE_DATA_SOURCE)
    @FlywayDataSource
    @ConfigurationProperties("fms.ride.datasource")
    public HikariDataSource rideDataSource() {
        return new HikariDataSource();
    }

    @Bean
    public JdbcClient rideJdbcClient(@Qualifier(RIDE_DATA_SOURCE) DataSource rideDataSource) {
        return JdbcClient.create(rideDataSource);
    }

    /**
     * JdbcTransactionManager 는 커밋·롤백 중의 SQLException 도 DataAccessException 으로 바꾼다(직렬화 실패 → 재시도 가능 예외 등).
     * DataSourceTransactionManager 는 모두 TransactionSystemException 으로 감싼다. 연결이 끊겨 커밋 결과를 모르는 경우는
     * 두 관리자 모두 TransactionSystemException 이 되며, GlobalExceptionHandler 가 503 으로 돌려준다.
     */
    @Bean
    public PlatformTransactionManager rideTransactionManager(@Qualifier(RIDE_DATA_SOURCE) DataSource rideDataSource) {
        return new JdbcTransactionManager(rideDataSource);
    }

    @Bean
    public TransactionTemplate rideTransactionTemplate(PlatformTransactionManager rideTransactionManager) {
        return new TransactionTemplate(rideTransactionManager);
    }

    /**
     * db 헬스는 ClickHouse 만 본다(자동 구성은 DataSource 마다 넣는다). 라이드 DB 장애로 앱 전체가 DOWN(→ 트래픽 차단)이
     * 되면 텔레매틱스 수집·돌발상황 경고까지 멈춘다. 라이드 DB 장애는 라이드 API 의 503 으로만 드러낸다.
     */
    @Bean
    public HealthContributor dbHealthContributor(DataSource dataSource) {
        return new DataSourceHealthIndicator(dataSource);
    }
}
