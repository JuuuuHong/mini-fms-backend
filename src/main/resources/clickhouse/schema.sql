-- 차량 텔레메트리 이력(시계열)
-- ReplacingMergeTree: 정렬 키(vehicle_id, event_time)가 같은 행은 병합 때 ingested_at 이 가장 큰 하나만 남는다.
--   Redis 멱등 판별(10분 윈도우)을 빠져나간 늦은 재전송까지 저장소 단계에서 한 번 더 거른다.
-- PARTITION BY 일 단위: 보존 기간(TTL)이 지난 파티션을 통째로 지워 삭제 비용이 작다.
-- LowCardinality: 차량 ID·엔진 상태처럼 종류가 적은 문자열을 사전 인코딩해 저장·필터 비용을 줄인다.
CREATE TABLE IF NOT EXISTS telematics_history
(
    vehicle_id    LowCardinality(String),
    event_time    DateTime64(3, 'UTC'),
    latitude      Float64,
    longitude     Float64,
    speed         Float32,
    battery_soc   Float32,
    engine_status LowCardinality(String),
    ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (vehicle_id, event_time)
TTL toDateTime(event_time) + INTERVAL 90 DAY;
