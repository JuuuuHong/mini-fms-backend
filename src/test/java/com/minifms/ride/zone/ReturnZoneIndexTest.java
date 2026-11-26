package com.minifms.ride.zone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.minifms.common.config.properties.ReturnZoneProperties;
import com.minifms.common.config.properties.ReturnZoneProperties.Zone;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 구역 판정. 좌표는 경도 x, 위도 y 인 단위 정사각형 격자로 둔다.
 * <pre>
 *   SERVICE    (0,0)-(10,10)
 *   DISCOUNT   (1,1)-(3,3)
 *   PENALTY    (2,2)-(4,4)     ← DISCOUNT 와 (2,2)-(3,3) 에서 겹침
 *   NO_RETURN  (2.5,2.5)-(5,5) ← 셋 다와 겹침
 *   DISCOUNT-B (20,20)-(21,21) ← 서비스 구역 밖에 따로 있는 주차 권장 구역
 * </pre>
 */
class ReturnZoneIndexTest {

    final ReturnZoneIndex index = new ReturnZoneIndex(new ReturnZoneProperties(List.of(
            zone("SERVICE", ZoneType.SERVICE_ZONE, 0, 0, 10, 10),
            zone("DISCOUNT", ZoneType.DISCOUNT_ZONE, 1, 1, 3, 3),
            zone("PENALTY", ZoneType.PENALTY_ZONE, 2, 2, 4, 4),
            zone("NO-RETURN", ZoneType.NO_RETURN_ZONE, 2.5, 2.5, 5, 5),
            zone("DISCOUNT-B", ZoneType.DISCOUNT_ZONE, 20, 20, 21, 21))));

    @Test
    void 한_구역에만_속하면_그_구역() {
        assertThat(judge(8, 8).zoneId()).isEqualTo("SERVICE");
        assertThat(judge(1.5, 1.5).zoneId()).isEqualTo("DISCOUNT");
        assertThat(judge(20.5, 20.5).zoneType()).isEqualTo(ZoneType.DISCOUNT_ZONE);
    }

    @Test
    void 겹치면_더_제한적인_구역이_이긴다() {
        assertThat(judge(2.2, 2.2).zoneType()).as("할인 ∩ 벌금").isEqualTo(ZoneType.PENALTY_ZONE);
        assertThat(judge(2.8, 2.8).zoneType()).as("할인 ∩ 벌금 ∩ 반납 불가").isEqualTo(ZoneType.NO_RETURN_ZONE);
        assertThat(judge(2.8, 2.8).returnable()).isFalse();
        assertThat(judge(2.8, 2.8).rejectionReason()).isEqualTo("NO_RETURN_ZONE");
    }

    @Test
    void 어느_구역에도_없으면_서비스_지역_밖() {
        ZoneJudgement outside = judge(15, 15);
        assertThat(outside).isEqualTo(ZoneJudgement.OUT_OF_SERVICE_AREA);
        assertThat(outside.returnable()).isFalse();
        assertThat(outside.rejectionReason()).isEqualTo("OUT_OF_SERVICE_AREA");
    }

    @Test
    void 경계_위의_점은_구역_안이다() {
        assertThat(judge(4, 2.2).zoneType()).as("벌금 구역 오른쪽 변").isEqualTo(ZoneType.PENALTY_ZONE);
        assertThat(judge(10, 10).zoneId()).as("서비스 구역 꼭짓점").isEqualTo("SERVICE");
        assertThat(judge(10.0001, 10).zoneType()).isNull();
    }

    @Test
    void 위도와_경도를_뒤바꾼_WKT_는_기동할_때_거부한다() {
        // 서울 시청(위도 37.56, 경도 126.97)을 "위도 경도" 순서로 잘못 적으면 y=126.97 로 위도 범위를 벗어난다.
        assertThatThrownBy(() -> new ReturnZoneIndex(new ReturnZoneProperties(List.of(new Zone("SWAPPED", "뒤바뀜",
                ZoneType.SERVICE_ZONE, "POLYGON((37.56 126.97, 37.57 126.97, 37.57 126.98, 37.56 126.98, 37.56 126.97))")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("경도 위도");
    }

    @Test
    void 자기_교차_다각형과_다각형이_아닌_도형과_중복_ID_는_거부한다() {
        assertThatThrownBy(() -> indexOf(new Zone("BOWTIE", "나비", ZoneType.SERVICE_ZONE,
                "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("올바르지 않습니다");
        assertThatThrownBy(() -> indexOf(new Zone("LINE", "선", ZoneType.SERVICE_ZONE, "LINESTRING(0 0, 1 1)")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("POLYGON");
        assertThatThrownBy(() -> indexOf(zone("DUP", ZoneType.SERVICE_ZONE, 0, 0, 1, 1), zone("DUP", ZoneType.PENALTY_ZONE, 0, 0, 1, 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("중복");
    }

    private ZoneJudgement judge(double x, double y) {
        return index.judge(y, x);
    }

    private static ReturnZoneIndex indexOf(Zone... zones) {
        return new ReturnZoneIndex(new ReturnZoneProperties(List.of(zones)));
    }

    private static Zone zone(String id, ZoneType type, double minX, double minY, double maxX, double maxY) {
        String wkt = "POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))".formatted(
                minX, minY, maxX, minY, maxX, maxY, minX, maxY, minX, minY);
        return new Zone(id, id, type, wkt);
    }
}
