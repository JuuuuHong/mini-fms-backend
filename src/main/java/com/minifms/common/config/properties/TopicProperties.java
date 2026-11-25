package com.minifms.common.config.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Kafka 토픽 이름과 파티션 설정.
 * 파티션 키는 항상 vehicleId 라서, 같은 차량의 메시지는 같은 파티션에서 순서대로 처리된다.
 */
@ConfigurationProperties(prefix = "fms.topics")
public record TopicProperties(
        @DefaultValue("telematics-raw-events") String telematicsRaw,
        @DefaultValue("telematics-raw-events.DLT") String telematicsDlt,
        @DefaultValue("vehicle-geofence-alerts") String geofenceAlerts,
        @DefaultValue("vehicle-commands") String commands,
        @DefaultValue("vehicle-command-acks") String commandAcks,
        @DefaultValue("vehicle-command-compensations") String commandCompensations,
        @DefaultValue("c-its-incident-alerts") String incidentAlerts,
        @DefaultValue("c-its-incident-results") String incidentResults,
        @DefaultValue("6") int partitions,
        @DefaultValue("1") int replicationFactor) {
}
