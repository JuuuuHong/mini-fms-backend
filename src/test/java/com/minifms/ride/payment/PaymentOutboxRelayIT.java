package com.minifms.ride.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.minifms.common.config.properties.PaymentOutboxProperties;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.support.IntegrationTestSupport;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.TimeoutException;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 결제 요청 아웃박스 릴레이. 앱 컨텍스트의 릴레이가 같은 행을 가져가지 않도록 같은 PostgreSQL 컨테이너에 데이터베이스를
 * 따로 만들어(같은 Flyway 스키마) 쓰고, Kafka 는 확인 시점·결과를 손으로 정할 수 있는 가짜로 바꾼다.
 */
class PaymentOutboxRelayIT extends IntegrationTestSupport {

    static final String DATABASE = "payment_outbox_relay_it";
    static HikariDataSource dataSource;
    static JdbcClient jdbc;
    static TransactionTemplate transactionTemplate;

    final PaymentOutboxRepository repository = new PaymentOutboxRepository(jdbc);
    final PaymentOutboxProperties properties =
            new PaymentOutboxProperties(Duration.ofMillis(100), 10, Duration.ofSeconds(2), Duration.ofDays(7), Duration.ofHours(1));
    final FmsMetrics metrics = new FmsMetrics(new SimpleMeterRegistry());
    List<UUID> rides;

    @BeforeAll
    static void createDatabase() {
        try (HikariDataSource adminDataSource = dataSource(POSTGRES.getJdbcUrl())) {
            JdbcClient admin = JdbcClient.create(adminDataSource);
            boolean exists = admin.sql("SELECT count(*) FROM pg_database WHERE datname = :name")
                    .param("name", DATABASE).query(Long.class).single() > 0;
            if (!exists) {
                admin.sql("CREATE DATABASE " + DATABASE).update();
            }
        }
        dataSource = dataSource(POSTGRES.getJdbcUrl().replaceFirst("/fms(\\?|$)", "/" + DATABASE + "$1"));
        Flyway.configure().dataSource(dataSource).load().migrate();
        jdbc = JdbcClient.create(dataSource);
        transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @AfterAll
    static void closeDatabase() {
        dataSource.close();
    }

    @BeforeEach
    void seed() {
        jdbc.sql("TRUNCATE payment_outbox, rides, devices CASCADE").update();
        rides = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            rides.add(appendPaymentRequest(Instant.now()));
        }
    }

    @Test
    void 브로커가_확인한_행만_발행_완료로_표시하고_나머지는_다음_주기에_그것만_다시_보낸다() {
        KafkaTemplate<String, String> flaky = kafka();
        when(flaky.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(flaky.send(anyString(), eq(key(1)), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("브로커 응답 없음")));

        assertThat(relay(flaky).relayOnce()).isEqualTo(2);
        assertThat(unpublishedRides()).containsExactly(rides.get(1));

        KafkaTemplate<String, String> healthy = kafkaConfirmingAll();
        assertThat(relay(healthy).relayOnce()).isEqualTo(1);
        verify(healthy).send(anyString(), eq(key(1)), anyString());
        verify(healthy, never()).send(anyString(), eq(key(0)), anyString());
        assertThat(unpublishedRides()).isEmpty();
    }

    @Test
    void 보내는_도중_send_가_실패하면_멈추고_앞서_확인된_것만_표시한다() {
        KafkaTemplate<String, String> broken = kafka();
        AtomicInteger calls = new AtomicInteger();
        when(broken.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            if (calls.incrementAndGet() >= 2) {
                throw new org.apache.kafka.common.KafkaException("메타데이터를 얻지 못함");
            }
            return CompletableFuture.completedFuture(null);
        });

        assertThat(relay(broken).relayOnce()).isEqualTo(1);
        assertThat(calls.get()).as("실패 뒤에는 더 보내지 않는다").isEqualTo(2);
        assertThat(unpublishedRides()).containsExactly(rides.get(1), rides.get(2));
    }

    @Test
    void 확인을_마감_안에_받지_못하면_표시하지_않는다() {
        KafkaTemplate<String, String> silent = kafka();
        when(silent.send(anyString(), anyString(), anyString())).thenReturn(new CompletableFuture<>());
        PaymentOutboxRelay relay = new PaymentOutboxRelay(repository, transactionTemplate, silent,
                new PaymentOutboxProperties(Duration.ofMillis(100), 10, Duration.ofMillis(300), Duration.ofDays(7), Duration.ofHours(1)),
                metrics, Clock.systemUTC());

        assertThat(relay.relayOnce()).isZero();
        assertThat(unpublishedRides()).containsExactlyElementsOf(rides);
    }

    @Test
    void 여러_릴레이가_동시에_돌아도_잠긴_행은_건너뛰어_같은_행을_함께_보내지_않는다() throws Exception {
        CompletableFuture<SendResult<String, String>> pending = new CompletableFuture<>();
        AtomicInteger slowSends = new AtomicInteger();
        KafkaTemplate<String, String> slow = kafka();
        when(slow.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            slowSends.incrementAndGet();
            return pending;
        });
        KafkaTemplate<String, String> fast = kafkaConfirmingAll();

        // A: 세 행을 잠그고 보낸 뒤 브로커 확인을 기다린다(트랜잭션 진행 중, 확인 마감 10초).
        PaymentOutboxRelay relayA = new PaymentOutboxRelay(repository, transactionTemplate, slow,
                new PaymentOutboxProperties(Duration.ofMillis(100), 10, Duration.ofSeconds(10), Duration.ofDays(7), Duration.ofHours(1)),
                metrics, Clock.systemUTC());
        CompletableFuture<Integer> runA = CompletableFuture.supplyAsync(relayA::relayOnce);
        await().atMost(Duration.ofSeconds(5)).until(() -> slowSends.get() == 3);

        // B: A 가 잠근 행은 건너뛴다.
        assertThat(relay(fast).relayOnce()).isZero();
        verify(fast, never()).send(anyString(), anyString(), anyString());

        pending.complete(null);
        assertThat(runA.get(10, TimeUnit.SECONDS)).isEqualTo(3);
        assertThat(unpublishedRides()).isEmpty();
    }

    @Test
    void 발행이_확인된_지_보존_기간이_지난_행만_지운다() {
        Instant now = Instant.now();
        jdbc.sql("UPDATE payment_outbox SET published_at = :at WHERE ride_id = :id")
                .param("at", Timestamp.from(now.minus(Duration.ofDays(8)))).param("id", rides.get(0)).update();
        jdbc.sql("UPDATE payment_outbox SET published_at = :at WHERE ride_id = :id")
                .param("at", Timestamp.from(now.minus(Duration.ofDays(1)))).param("id", rides.get(1)).update();

        assertThat(repository.purgePublishedBefore(now.minus(Duration.ofDays(7)))).isEqualTo(1);
        assertThat(jdbc.sql("SELECT ride_id FROM payment_outbox ORDER BY id").query(UUID.class).list())
                .containsExactly(rides.get(1), rides.get(2));
    }

    private PaymentOutboxRelay relay(KafkaTemplate<String, String> kafka) {
        return new PaymentOutboxRelay(repository, transactionTemplate, kafka, properties, metrics, Clock.systemUTC());
    }

    /** 기기·라이드(외래 키)와 결제 요청 한 건을 넣는다. 키 = rideId. */
    private UUID appendPaymentRequest(Instant now) {
        String deviceId = "D-" + UUID.randomUUID().toString().substring(0, 8);
        UUID rideId = UUID.randomUUID();
        jdbc.sql("INSERT INTO devices (device_id, state, state_changed_at, registered_at) VALUES (:id, 'AVAILABLE', :now, :now)")
                .param("id", deviceId).param("now", Timestamp.from(now)).update();
        jdbc.sql("""
                        INSERT INTO rides (ride_id, user_id, device_id, status, started_at, unlock_fee, per_minute_fee,
                                           unlock_fee_waived, riding_discount_percent)
                        VALUES (:rideId, :userId, :deviceId, 'IN_PROGRESS', :now, 1000, 150, false, 0)""")
                .param("rideId", rideId).param("userId", "U-" + deviceId).param("deviceId", deviceId)
                .param("now", Timestamp.from(now)).update();
        repository.append(UUID.randomUUID(), rideId, "ride-payment-requests", rideId.toString(),
                "{\"rideId\":\"" + rideId + "\",\"amount\":1000}", now);
        return rideId;
    }

    private String key(int index) {
        return rides.get(index).toString();
    }

    private List<UUID> unpublishedRides() {
        return jdbc.sql("SELECT ride_id FROM payment_outbox WHERE published_at IS NULL ORDER BY id").query(UUID.class).list();
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafka() {
        return mock(KafkaTemplate.class);
    }

    private static KafkaTemplate<String, String> kafkaConfirmingAll() {
        KafkaTemplate<String, String> kafka = kafka();
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        return kafka;
    }

    private static HikariDataSource dataSource(String jdbcUrl) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(jdbcUrl);
        ds.setUsername(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        ds.setMaximumPoolSize(4);
        return ds;
    }
}
