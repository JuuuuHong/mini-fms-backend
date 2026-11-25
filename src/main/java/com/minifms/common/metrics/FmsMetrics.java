package com.minifms.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * 파이프라인 지표. /actuator/metrics/{name} 으로 확인한다.
 * 예: /actuator/metrics/fms.telematics.duplicates?tag=sink:history
 */
@Component
public class FmsMetrics {

    public static final String INGESTED = "fms.telematics.ingested";
    public static final String PROCESSED = "fms.telematics.processed";
    public static final String DUPLICATES = "fms.telematics.duplicates";
    public static final String INVALID = "fms.telematics.invalid";
    public static final String OUT_OF_ORDER = "fms.telematics.out_of_order";
    public static final String GEOFENCE_TRANSITIONS = "fms.geofence.transitions";
    public static final String COMMANDS_FINISHED = "fms.command.finished";
    public static final String COMMAND_LATE_ACKS = "fms.command.late_acks";
    public static final String OUTBOX_PUBLISHED = "fms.outbox.published";
    public static final String INCIDENT_HISTORY_INDEXED = "fms.incident.history.indexed";
    public static final String INCIDENT_HISTORY_STALE = "fms.incident.history.stale";
    public static final String INCIDENT_HISTORY_REJECTED = "fms.incident.history.rejected";

    private final MeterRegistry registry;

    public FmsMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void ingested(int count) {
        increment(registry.counter(INGESTED), count);
    }

    public void processed(String sink, int count) {
        increment(registry.counter(PROCESSED, "sink", sink), count);
    }

    public void duplicates(String sink, int count) {
        increment(registry.counter(DUPLICATES, "sink", sink), count);
    }

    public void invalid(String sink, int count) {
        increment(registry.counter(INVALID, "sink", sink), count);
    }

    public void outOfOrder() {
        registry.counter(OUT_OF_ORDER).increment();
    }

    public void geofenceTransition(String geofenceId, String transition) {
        registry.counter(GEOFENCE_TRANSITIONS, "geofence", geofenceId, "transition", transition).increment();
    }

    public void commandFinished(String type, String status) {
        registry.counter(COMMANDS_FINISHED, "type", type, "status", status).increment();
    }

    public void lateAck() {
        registry.counter(COMMAND_LATE_ACKS).increment();
    }

    public void outboxPublished(int count) {
        increment(registry.counter(OUTBOX_PUBLISHED), count);
    }

    /** 이력 색인에 반영된 처리 결과 수. */
    public void incidentHistoryIndexed(int count) {
        increment(registry.counter(INCIDENT_HISTORY_INDEXED), count);
    }

    /** 이미 같은·더 최신 버전이 있어 버린 처리 결과 수(재전달·늦은 도착). */
    public void incidentHistoryStale(int count) {
        increment(registry.counter(INCIDENT_HISTORY_STALE), count);
    }

    /** 재시도해도 소용없는 이유로 색인하지 못한 처리 결과 수(해석 불가·매핑 오류). */
    public void incidentHistoryRejected(int count) {
        increment(registry.counter(INCIDENT_HISTORY_REJECTED), count);
    }

    /** 테스트·진단용: 현재 누적값. */
    public double count(String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0 : counter.count();
    }

    private static void increment(Counter counter, int count) {
        if (count > 0) {
            counter.increment(count);
        }
    }
}
