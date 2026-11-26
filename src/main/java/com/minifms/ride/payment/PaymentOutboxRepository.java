package com.minifms.ride.payment;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** 결제 요청 아웃박스 테이블(payment_outbox). */
@Repository
public class PaymentOutboxRepository {

    private final JdbcClient jdbc;

    public PaymentOutboxRepository(JdbcClient rideJdbcClient) {
        this.jdbc = rideJdbcClient;
    }

    /** 반납 확정 트랜잭션 안에서 부른다. 라이드당 한 건(ride_id 유니크). */
    public void append(UUID eventId, UUID rideId, String topic, String key, String payloadJson, Instant now) {
        jdbc.sql("""
                        INSERT INTO payment_outbox (event_id, ride_id, topic, message_key, payload, created_at)
                        VALUES (:eventId, :rideId, :topic, :key, CAST(:payload AS jsonb), :now)""")
                .param("eventId", eventId)
                .param("rideId", rideId)
                .param("topic", topic)
                .param("key", key)
                .param("payload", payloadJson)
                .param("now", Timestamp.from(now))
                .update();
    }

    /**
     * 아직 발행하지 않은 행을 오래된 순으로 잠근다. 트랜잭션 안에서 불러야 하며, 커밋·롤백할 때까지 다른 릴레이는
     * 이 행들을 건너뛴다(SKIP LOCKED) — 여러 인스턴스가 함께 릴레이해도 같은 행을 동시에 보내지 않는다.
     */
    public List<OutboxRow> lockUnpublished(int limit) {
        return jdbc.sql("""
                        SELECT id, topic, message_key, payload::text AS payload FROM payment_outbox
                        WHERE published_at IS NULL
                        ORDER BY id
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED""")
                .param("limit", limit)
                .query((rs, rowNum) -> new OutboxRow(
                        rs.getLong("id"), rs.getString("topic"), rs.getString("message_key"), rs.getString("payload")))
                .list();
    }

    public int markPublished(List<Long> ids, Instant now) {
        if (ids.isEmpty()) {
            return 0;
        }
        return jdbc.sql("UPDATE payment_outbox SET published_at = :now WHERE id IN (:ids) AND published_at IS NULL")
                .param("now", Timestamp.from(now))
                .param("ids", ids)
                .update();
    }

    /** 발행이 확인된 지 오래된 행을 지운다. 지운 수를 돌려준다. */
    public int purgePublishedBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM payment_outbox WHERE published_at < :cutoff")
                .param("cutoff", Timestamp.from(cutoff))
                .update();
    }

    public record OutboxRow(long id, String topic, String key, String payload) {
    }
}
