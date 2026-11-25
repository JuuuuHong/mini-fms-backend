package com.minifms.incident;

/** 돌발상황 한 건의 경고 전파 처리 상태. "발행 확인"은 Kafka 브로커가 수신을 확인(acks=all)한 것이다. */
public enum IncidentDispatchStatus {
    /** 반경 안에 주행 중인 차량이 없어 보낼 경고가 없었다. */
    NO_TARGETS,
    /** 대상 차량 모두의 경고 발행이 확인됐다. */
    COMPLETED,
    /** 일부만 확인됐다. 접수 API 는 503 을 돌려 보고 측이 재시도하게 한다. */
    PARTIALLY_FAILED,
    /** 하나도 확인되지 않았다(브로커 장애 등). 접수 API 는 503. */
    FAILED;

    public static IncidentDispatchStatus of(int targetCount, int confirmedCount) {
        if (targetCount == 0) {
            return NO_TARGETS;
        }
        if (confirmedCount >= targetCount) {
            return COMPLETED;
        }
        return confirmedCount == 0 ? FAILED : PARTIALLY_FAILED;
    }
}
