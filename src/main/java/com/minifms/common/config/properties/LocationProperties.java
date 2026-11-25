package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 실시간 위치 저장소(Redis GEO) 설정.
 *
 * @param activeThreshold   마지막 수신이 이 시간 안이면 "활성 차량"으로 본다(반경 조회 대상).
 * @param staleAfter        마지막 수신이 이 시간보다 오래되면 GEO 인덱스에서 뺀다.
 * @param evictionInterval  오래된 차량 정리 주기.
 * @param latestTtl         차량별 최신 상태 해시의 보존 기간.
 * @param maxNearbyResults  반경 조회 한 번에 돌려주는 최대 차량 수.
 */
@ConfigurationProperties(prefix = "fms.location")
public record LocationProperties(
        @DefaultValue("60s") Duration activeThreshold,
        @DefaultValue("10m") Duration staleAfter,
        @DefaultValue("1m") Duration evictionInterval,
        @DefaultValue("7d") Duration latestTtl,
        @DefaultValue("500") int maxNearbyResults) {
}
