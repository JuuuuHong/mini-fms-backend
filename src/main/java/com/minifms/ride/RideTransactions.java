package com.minifms.ride;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.web.ConflictException;
import com.minifms.ride.device.DeviceRepository;
import com.minifms.ride.fare.Fare;
import com.minifms.ride.payment.PaymentOutboxRepository;
import com.minifms.ride.payment.PaymentRequestedEvent;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 라이드의 로컬 트랜잭션(PostgreSQL). 분산 락 없이 불러도(락을 잃은 경합) 상태 전이가 한 번만 일어나도록,
 * 모든 갱신이 "현재 상태가 기대한 값일 때만" 바꾸는 조건부 갱신이다.
 */
@Component
public class RideTransactions {

    private final TransactionTemplate transactionTemplate;
    private final RideRepository rides;
    private final DeviceRepository devices;
    private final PaymentOutboxRepository paymentOutbox;
    private final ObjectMapper objectMapper;
    private final TopicProperties topics;

    public RideTransactions(TransactionTemplate rideTransactionTemplate, RideRepository rides, DeviceRepository devices,
                            PaymentOutboxRepository paymentOutbox, ObjectMapper objectMapper, TopicProperties topics) {
        this.transactionTemplate = rideTransactionTemplate;
        this.rides = rides;
        this.devices = devices;
        this.paymentOutbox = paymentOutbox;
        this.objectMapper = objectMapper;
        this.topics = topics;
    }

    /**
     * 대여: 기기 AVAILABLE → IN_USE + 라이드 생성.
     *
     * @throws ConflictException 기기가 이미 AVAILABLE 이 아님(다른 대여가 먼저 확정)
     * @throws org.springframework.dao.DuplicateKeyException 사용자에게 진행 중 라이드가 이미 있음
     */
    public void start(Ride ride) {
        transactionTemplate.executeWithoutResult(status -> {
            if (!devices.markInUse(ride.deviceId(), ride.rideId(), ride.startedAt())) {
                throw new ConflictException("이미 대여 중이거나 대여할 수 없는 기기입니다.",
                        Map.of("deviceId", ride.deviceId(), "reason", "DEVICE_NOT_AVAILABLE"));
            }
            rides.insert(ride);
        });
    }

    /**
     * 반납 확정: 라이드 IN_PROGRESS → COMPLETED + 기기 IN_USE → LOCKED + 결제 요청 아웃박스 적재.
     * 셋이 함께 커밋되거나 함께 롤백된다. 합계가 0원이면 청구할 것이 없으므로 결제 요청을 만들지 않는다
     * (PG 는 0원 승인을 받지 않는다). 0원 라이드는 라이드 기록의 total_fare = 0 으로만 남는다.
     *
     * @return 이번 호출이 확정했으면 true. 다른 요청이 이미 확정했으면 false(아무것도 바꾸지 않음).
     */
    public boolean complete(Ride ride, RideReturn ret, Fare fare, Instant now) {
        Boolean completed = transactionTemplate.execute(status -> {
            if (!rides.complete(ride.rideId(), ret, fare)) {
                return false;
            }
            if (!devices.markLocked(ride.deviceId(), ride.rideId(), now)) {
                // 라이드는 진행 중인데 기기가 그 라이드의 IN_USE 가 아니다 — 같은 트랜잭션으로만 바꾸므로 생기면 안 되는 상태.
                throw new IllegalStateException("기기 상태가 라이드와 맞지 않습니다 rideId=" + ride.rideId()
                        + " deviceId=" + ride.deviceId());
            }
            if (fare.total() == 0) {
                return true;
            }
            Ride completedRide = new Ride(ride.rideId(), ride.userId(), ride.deviceId(), RideStatus.COMPLETED,
                    ride.startedAt(), ride.tariff(), ret, fare);
            UUID eventId = UUID.randomUUID();
            PaymentRequestedEvent event = PaymentRequestedEvent.of(eventId, completedRide, now);
            paymentOutbox.append(eventId, ride.rideId(), topics.paymentRequests(), ride.rideId().toString(),
                    toJson(event), now);
            return true;
        });
        return Boolean.TRUE.equals(completed);
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("결제 요청 직렬화 실패", e);
        }
    }
}
