package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 멱등 처리 설정.
 *
 * @param window 처리 완료(DONE) 표시를 유지하는 기간. 이 기간 안에 다시 온 같은 메시지는 중복으로 버린다.
 * @param lease  처리 중(선점) 표시의 유효 기간. 소비자가 죽어 선점을 풀지 못해도 이 시간이 지나면 다시 처리할 수 있다.
 */
@ConfigurationProperties(prefix = "fms.idempotency")
public record IdempotencyProperties(
        @DefaultValue("10m") Duration window,
        @DefaultValue("15s") Duration lease) {
}
