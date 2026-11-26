package com.minifms.ride.zone;

import com.minifms.common.config.properties.ReturnZoneProperties;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.algorithm.locate.SimplePointInAreaLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.springframework.stereotype.Component;

/**
 * 반납 구역 판정기. 기동할 때 구역 다각형을 읽어 STRtree(경계 사각형 R-tree)에 넣고, 판정은
 * "좌표를 포함할 수 있는 후보(사각형 검사) → 실제 다각형 포함 검사" 두 단계로 한다. 구역이 많아도 후보만 정밀 검사한다.
 *
 * <ul>
 *   <li><b>경계 위의 점은 구역 안</b>으로 본다(covers). 벌금·반납 불가 구역도 경계에 닿으면 해당한다.</li>
 *   <li>좌표는 경도·위도를 평면 좌표로 그대로 쓴다. 도시 규모(수 km) 다각형에서는 측지선과의 차이가 GPS 오차보다 훨씬 작다.</li>
 *   <li>겹친 구역은 {@link ZoneType#precedence()} 가 큰 구역 하나를 고른다. 같은 정책끼리 겹치면 ID 순으로 앞선 것.</li>
 * </ul>
 * 색인은 만든 뒤 바뀌지 않는다(STRtree 는 build 뒤 조회만 하면 여러 스레드가 함께 써도 된다).
 */
@Component
public class ReturnZoneIndex {

    /** 좌표 순서를 정밀도 손실 없이 그대로 쓰고, WGS84(EPSG:4326)임을 표시한다. */
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel(), 4326);
    private static final Comparator<ReturnZone> PRIORITY = Comparator
            .comparingInt((ReturnZone zone) -> zone.type().precedence()).reversed()
            .thenComparing(ReturnZone::id);

    private final List<ReturnZone> zones;
    private final STRtree index = new STRtree();

    public ReturnZoneIndex(ReturnZoneProperties properties) {
        List<ReturnZone> parsed = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        WKTReader reader = new WKTReader(GEOMETRY_FACTORY);
        for (ReturnZoneProperties.Zone zone : properties.zones()) {
            if (!ids.add(zone.id())) {
                throw new IllegalArgumentException("반납 구역 ID 가 중복됩니다: " + zone.id());
            }
            ReturnZone returnZone = new ReturnZone(zone.id(), zone.name(), zone.type(), parse(reader, zone));
            parsed.add(returnZone);
            index.insert(returnZone.geometry().getEnvelopeInternal(), returnZone);
        }
        index.build();
        this.zones = List.copyOf(parsed);
    }

    /** 좌표가 속한 구역 중 가장 제한적인 구역. 어느 구역에도 속하지 않으면 {@link ZoneJudgement#OUT_OF_SERVICE_AREA}. */
    public ZoneJudgement judge(double latitude, double longitude) {
        Coordinate point = new Coordinate(longitude, latitude);
        @SuppressWarnings("unchecked")
        List<ReturnZone> candidates = index.query(new Envelope(point));
        return candidates.stream()
                .filter(zone -> SimplePointInAreaLocator.locate(point, zone.geometry()) != Location.EXTERIOR)
                .min(PRIORITY)
                .map(zone -> new ZoneJudgement(zone.id(), zone.name(), zone.type()))
                .orElse(ZoneJudgement.OUT_OF_SERVICE_AREA);
    }

    public List<ReturnZone> zones() {
        return zones;
    }

    private static Geometry parse(WKTReader reader, ReturnZoneProperties.Zone zone) {
        Geometry geometry;
        try {
            geometry = reader.read(zone.polygon());
        } catch (ParseException e) {
            throw new IllegalArgumentException("반납 구역 " + zone.id() + " 의 WKT 를 읽을 수 없습니다: " + e.getMessage(), e);
        }
        if (!(geometry instanceof Polygonal) || geometry.isEmpty()) {
            throw new IllegalArgumentException("반납 구역 " + zone.id() + " 은 POLYGON 또는 MULTIPOLYGON 이어야 합니다: "
                    + geometry.getGeometryType());
        }
        if (!geometry.isValid()) {
            throw new IllegalArgumentException("반납 구역 " + zone.id() + " 의 다각형이 올바르지 않습니다(자기 교차 등)");
        }
        Envelope envelope = geometry.getEnvelopeInternal();
        if (envelope.getMinX() < -180 || envelope.getMaxX() > 180 || envelope.getMinY() < -90 || envelope.getMaxY() > 90) {
            throw new IllegalArgumentException("반납 구역 " + zone.id() + " 의 좌표가 경도·위도 범위를 벗어납니다"
                    + "(WKT 는 \"경도 위도\" 순서): " + envelope);
        }
        return geometry;
    }

    public record ReturnZone(String id, String name, ZoneType type, Geometry geometry) {
    }
}
