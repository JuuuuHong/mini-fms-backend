package com.minifms.common.redis;

/**
 * Redis 키 규칙을 한곳에 모은다. vehicleId·fenceId 는 입력 검증에서 [A-Za-z0-9_-] 만 허용하므로
 * 키 구분자(:)가 끼어들 수 없다.
 *
 * <p>여러 키를 한 Lua 스크립트에서 다루므로 단일 Redis(또는 Sentinel) 전제다. Redis Cluster 로 옮기면
 * 같은 스크립트가 다루는 키에 hash tag({vehicleId})를 붙여 같은 슬롯에 모아야 한다.
 */
public final class RedisKeys {

    /** 차량 최신 위치 GEO 인덱스(sorted set). member = vehicleId. */
    public static final String VEHICLE_GEO = "fms:vehicles:geo";
    /** 차량별 마지막 수신 시각(sorted set). score = 이벤트 timestamp(ms). 활성 판정·정리에 쓴다. */
    public static final String VEHICLE_LAST_SEEN = "fms:vehicles:last-seen";
    /** 응답 대기 중인 명령의 마감 시각(sorted set). score = deadline(ms), member = commandId. */
    public static final String COMMAND_DEADLINES = "fms:command:deadlines";
    /** Kafka 로 나갈 이벤트를 상태 변경과 같은 Lua 스크립트 안에서 쌓아 두는 아웃박스(list). */
    public static final String OUTBOX = "fms:outbox";
    /** 해석할 수 없어 발행하지 못한 아웃박스 항목. */
    public static final String OUTBOX_DEAD = "fms:outbox:dead";
    /** 아웃박스 릴레이 잠금. */
    public static final String OUTBOX_RELAY_LOCK = "fms:outbox:relay-lock";

    private RedisKeys() {
    }

    /** 차량 최신 텔레메트리(hash). */
    public static String vehicleLatest(String vehicleId) {
        return "fms:vehicle:" + vehicleId + ":latest";
    }

    /** 원격 제어로 바뀌는 차량 제어 상태(hash: door, engine). */
    public static String vehicleShadow(String vehicleId) {
        return "fms:vehicle:" + vehicleId + ":shadow";
    }

    /** 차량별 진행 중 명령 잠금(string, 값 = commandId). */
    public static String vehicleCommandLock(String vehicleId) {
        return "fms:vehicle:" + vehicleId + ":command-lock";
    }

    /** 명령 상태(hash). */
    public static String command(String commandId) {
        return "fms:command:" + commandId;
    }

    /** 지오펜스 안에 있는 차량 집합(set). */
    public static String geofenceInside(String fenceId) {
        return "fms:geofence:" + fenceId + ":inside";
    }

    /** 멱등키(string). 값은 처리 토큰(선점 중) 또는 DONE(처리 완료). */
    public static String idempotency(String namespace, String businessKey) {
        return "fms:dedup:" + namespace + ":" + businessKey;
    }
}
