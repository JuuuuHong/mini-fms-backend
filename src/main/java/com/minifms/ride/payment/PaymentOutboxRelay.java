package com.minifms.ride.payment;

import com.minifms.common.config.properties.PaymentOutboxProperties;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.ride.payment.PaymentOutboxRepository.OutboxRow;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 결제 요청 아웃박스(PostgreSQL) → Kafka 릴레이(at-least-once).
 *
 * <p>반납 API 는 외부 결제를 직접 부르지도, Kafka 에 직접 보내지도 않는다. 라이드 확정과 결제 요청 적재가 한 DB 트랜잭션이라
 * "요금은 확정됐는데 결제 요청이 없음" 또는 "결제 요청은 나갔는데 라이드는 롤백됨"이 생기지 않는다. 브로커가 멈춰도 반납은
 * 성공하고, 결제 요청은 이 릴레이가 브로커 복구 뒤 보낸다.
 *
 * <ul>
 *   <li>한 주기 = 한 트랜잭션: 미발행 행을 {@code FOR UPDATE SKIP LOCKED} 로 잠그고 → 모두 보낸 뒤 → 브로커 확인(acks=all)이
 *       온 행만 published_at 을 찍고 커밋한다. 확인되지 않은 행은 다음 주기에 다시 보낸다.</li>
 *   <li>여러 인스턴스가 함께 돌아도 잠긴 행은 서로 건너뛰므로 같은 행을 동시에 보내지 않는다(잠금 서버 불필요).</li>
 *   <li>보낸 뒤 확인 전에 프로세스가 죽으면 트랜잭션이 롤백돼 다시 보낸다 — 중복은 가능, 유실은 없다.
 *       소비자는 eventId·idempotencyKey 로 거른다.</li>
 *   <li>라이드당 결제 이벤트는 하나라 행끼리의 순서는 의미가 없다(키 = rideId).</li>
 * </ul>
 */
@Component
public class PaymentOutboxRelay implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(PaymentOutboxRelay.class);

    private final PaymentOutboxRepository repository;
    private final TransactionTemplate transactionTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PaymentOutboxProperties properties;
    private final FmsMetrics metrics;
    private final Clock clock;

    public PaymentOutboxRelay(PaymentOutboxRepository repository, TransactionTemplate rideTransactionTemplate,
                              KafkaTemplate<String, String> kafkaTemplate, PaymentOutboxProperties properties,
                              FmsMetrics metrics, Clock clock) {
        this.repository = repository;
        this.transactionTemplate = rideTransactionTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(this::relaySafely, properties.relayInterval());
        registrar.addFixedDelayTask(this::purgeSafely, properties.purgeInterval());
    }

    private void relaySafely() {
        try {
            relayOnce();
        } catch (RuntimeException e) {
            log.warn("결제 요청 릴레이 실패, 다음 주기에 재시도합니다: {}", e.toString());
        }
    }

    private void purgeSafely() {
        try {
            int purged = repository.purgePublishedBefore(clock.instant().minus(properties.retention()));
            if (purged > 0) {
                log.info("발행이 확인된 지 {} 지난 결제 요청 {}건을 아웃박스에서 지웠습니다", properties.retention(), purged);
            }
        } catch (RuntimeException e) {
            log.warn("결제 요청 아웃박스 정리 실패: {}", e.toString());
        }
    }

    /** 한 묶음을 보내고, 브로커 확인이 와서 발행 완료로 표시한 행 수를 돌려준다. */
    public int relayOnce() {
        Integer published = transactionTemplate.execute(status -> {
            List<OutboxRow> rows = repository.lockUnpublished(properties.batchSize());
            if (rows.isEmpty()) {
                return 0;
            }
            List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
            for (OutboxRow row : rows) {
                try {
                    sends.add(kafkaTemplate.send(row.topic(), row.key(), row.payload()));
                } catch (RuntimeException e) {
                    // send 자체가 실패(브로커 메타데이터를 못 얻음 등)하면 남은 행은 보내지 않고 앞서 보낸 것만 확인한다.
                    log.warn("결제 요청 발행 중단(브로커 장애 추정), 나머지는 다음 주기에 보냅니다: {}", e.toString());
                    break;
                }
            }

            long deadline = System.nanoTime() + properties.sendTimeout().toNanos();
            List<Long> confirmed = new ArrayList<>(sends.size());
            for (int i = 0; i < sends.size(); i++) {
                try {
                    sends.get(i).get(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    confirmed.add(rows.get(i).id());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // 행끼리 순서가 없으므로 실패한 행만 남기고 나머지 확인은 계속 본다.
                    log.warn("결제 요청 발행 확인 실패 outboxId={}, 다음 주기에 다시 보냅니다: {}", rows.get(i).id(), e.toString());
                }
            }
            return repository.markPublished(confirmed, clock.instant());
        });
        int count = published == null ? 0 : published;
        metrics.paymentRequestsPublished(count);
        return count;
    }
}
