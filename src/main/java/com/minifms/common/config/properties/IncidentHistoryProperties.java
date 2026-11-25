package com.minifms.common.config.properties;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 돌발상황 관제 이력(Elasticsearch) 설정.
 *
 * @param index          색인 이름. 문서 ID = incidentId.
 * @param maxSearchRange 한 번에 검색할 수 있는 발생 시각 범위의 최대 길이.
 * @param maxPageSize    한 페이지 최대 건수.
 * @param versionTtl     incidentId 별 처리 결과 버전 카운터 보존 기간(Redis).
 */
@ConfigurationProperties(prefix = "fms.incident-history")
public record IncidentHistoryProperties(
        @DefaultValue("c-its-incidents") String index,
        @DefaultValue("31d") Duration maxSearchRange,
        @DefaultValue("100") int maxPageSize,
        @DefaultValue("30d") Duration versionTtl) {
}
