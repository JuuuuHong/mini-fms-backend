-- 라이드 정산(PostgreSQL). 기기 상태 전이·라이드 확정·결제 요청 적재를 한 트랜잭션으로 묶는다.
-- 금액은 원 단위 정수(BIGINT), 시각은 앱의 Clock 으로 넣는다(TIMESTAMPTZ).

-- 기기 상태 머신: AVAILABLE ─대여→ IN_USE ─반납→ LOCKED ─잠금 명령 ACK→ AVAILABLE
CREATE TABLE devices (
    device_id        VARCHAR(64)  PRIMARY KEY,
    state            VARCHAR(16)  NOT NULL CHECK (state IN ('AVAILABLE', 'IN_USE', 'LOCKED')),
    current_ride_id  UUID,
    -- LOCKED 동안의 잠금 명령 진행 상황(DeviceLockReconciler)
    lock_command_id  VARCHAR(36),
    lock_attempts    INT          NOT NULL DEFAULT 0,
    lock_retry_at    TIMESTAMPTZ,
    state_changed_at TIMESTAMPTZ  NOT NULL,
    registered_at    TIMESTAMPTZ  NOT NULL,
    CHECK ((state = 'IN_USE') = (current_ride_id IS NOT NULL)),
    CHECK (state = 'LOCKED' OR (lock_command_id IS NULL AND lock_retry_at IS NULL))
);
CREATE INDEX devices_lock_due ON devices (lock_retry_at) WHERE state = 'LOCKED';

-- 구독권(판매·결제는 범위 밖, 여기서는 읽기만 한다)
CREATE TABLE subscriptions (
    subscription_id BIGSERIAL    PRIMARY KEY,
    user_id         VARCHAR(64)  NOT NULL,
    plan_code       VARCHAR(32)  NOT NULL,
    starts_at       TIMESTAMPTZ  NOT NULL,
    ends_at         TIMESTAMPTZ  NOT NULL,
    CHECK (ends_at > starts_at)
);
CREATE INDEX subscriptions_user_period ON subscriptions (user_id, ends_at);

CREATE TABLE rides (
    ride_id                 UUID         PRIMARY KEY,
    user_id                 VARCHAR(64)  NOT NULL,
    device_id               VARCHAR(64)  NOT NULL REFERENCES devices (device_id),
    status                  VARCHAR(16)  NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED')),
    started_at              TIMESTAMPTZ  NOT NULL,
    -- 대여 시점의 요금표·구독권 스냅샷(이용 중에 요금표가 바뀌어도 대여 때 기준으로 정산)
    unlock_fee              BIGINT       NOT NULL,
    per_minute_fee          BIGINT       NOT NULL,
    subscription_plan       VARCHAR(32),
    unlock_fee_waived       BOOLEAN      NOT NULL,
    riding_discount_percent INT          NOT NULL CHECK (riding_discount_percent BETWEEN 0 AND 100),
    -- 반납 판정
    ended_at                TIMESTAMPTZ,
    end_latitude            DOUBLE PRECISION,
    end_longitude           DOUBLE PRECISION,
    location_measured_at    TIMESTAMPTZ,
    zone_id                 VARCHAR(64),
    zone_type               VARCHAR(16),
    -- 요금: 잠금해제료 + 주행요금 + 구역 조정 - 구독권 할인 = 합계
    riding_minutes          BIGINT,
    riding_fee              BIGINT,
    zone_adjustment         BIGINT,
    subscription_discount   BIGINT,
    total_fare              BIGINT,
    CHECK (status = 'IN_PROGRESS' OR (ended_at IS NOT NULL AND total_fare IS NOT NULL)),
    CHECK (total_fare IS NULL
        OR total_fare = unlock_fee + riding_fee + zone_adjustment - subscription_discount)
);
-- 기기·사용자당 진행 중 라이드는 하나. 분산 락을 잃은 경합에서도 DB 가 마지막으로 막는다.
CREATE UNIQUE INDEX rides_active_device ON rides (device_id) WHERE status = 'IN_PROGRESS';
CREATE UNIQUE INDEX rides_active_user ON rides (user_id) WHERE status = 'IN_PROGRESS';

-- 결제 요청 아웃박스(Transactional Outbox). 반납 확정과 같은 트랜잭션에서 적재하고 PaymentOutboxRelay 가 Kafka 로 보낸다.
CREATE TABLE payment_outbox (
    id           BIGSERIAL    PRIMARY KEY,
    event_id     UUID         NOT NULL UNIQUE,
    ride_id      UUID         NOT NULL UNIQUE REFERENCES rides (ride_id),  -- 라이드당 결제 요청 1건
    topic        VARCHAR(128) NOT NULL,
    message_key  VARCHAR(128) NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ
);
CREATE INDEX payment_outbox_unpublished ON payment_outbox (id) WHERE published_at IS NULL;
CREATE INDEX payment_outbox_published_at ON payment_outbox (published_at) WHERE published_at IS NOT NULL;
