package com.minifms.common.config;

import com.minifms.common.config.properties.TopicProperties;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * 토픽은 앱이 기동할 때 KafkaAdmin 이 만든다(이미 있으면 그대로 둔다).
 * 브로커의 auto.create.topics 는 꺼 두어, 오타 난 토픽이 기본 설정으로 생기는 일을 막는다.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public KafkaAdmin.NewTopics fmsTopics(TopicProperties topics) {
        return new KafkaAdmin.NewTopics(
                topic(topics, topics.telematicsRaw(), Duration.ofDays(3)),
                topic(topics, topics.telematicsDlt(), Duration.ofDays(14)),
                topic(topics, topics.geofenceAlerts(), Duration.ofDays(7)),
                topic(topics, topics.commands(), Duration.ofDays(1)),
                topic(topics, topics.commandAcks(), Duration.ofDays(1)),
                topic(topics, topics.commandCompensations(), Duration.ofDays(7)),
                topic(topics, topics.incidentAlerts(), Duration.ofDays(1)),
                // 색인 소비자가 Elasticsearch 장애로 밀려도 복구 뒤 처리할 수 있게 넉넉히 보존한다.
                topic(topics, topics.incidentResults(), Duration.ofDays(7)));
    }

    private static NewTopic topic(TopicProperties topics, String name, Duration retention) {
        return TopicBuilder.name(name)
                .partitions(topics.partitions())
                .replicas(topics.replicationFactor())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retention.toMillis()))
                .build();
    }
}
