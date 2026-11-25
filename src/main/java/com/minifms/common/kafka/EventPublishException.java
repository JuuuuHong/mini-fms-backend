package com.minifms.common.kafka;

/**
 * Kafka 발행 실패.
 *
 * <p>{@link #outcomeUnknown()} 이 true 면 "실패했다"가 아니라 "브로커가 받았는지 모른다"는 뜻이다(확인 대기 시간 초과,
 * 재시도 가능한 브로커 오류). 레코드가 이미 기록돼 소비자에게 전달될 수 있으므로, 받는 쪽이 실행할 수 있는 메시지
 * (예: 차량 제어 명령)는 이 경우 실패로 확정해 보상하면 안 된다.
 */
public class EventPublishException extends RuntimeException {

    private final boolean outcomeUnknown;

    public EventPublishException(String message, Throwable cause) {
        this(message, cause, false);
    }

    public EventPublishException(String message, Throwable cause, boolean outcomeUnknown) {
        super(message, cause);
        this.outcomeUnknown = outcomeUnknown;
    }

    public boolean outcomeUnknown() {
        return outcomeUnknown;
    }
}
