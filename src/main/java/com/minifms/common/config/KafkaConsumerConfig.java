package com.minifms.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * 텔레매틱스 소비자용 배치 리스너 팩토리.
 *
 * <p>실시간 위치(Redis)와 과거 궤적(ClickHouse)은 서로 다른 consumer group 으로 같은 토픽을 읽는다.
 * ClickHouse 장애가 실시간 위치 갱신을 막지 않게 하기 위해서이며, 장애 대응 정책도 다르다.
 * <ul>
 *   <li>실시간: 위치는 곧 새 값으로 덮이므로 제한된 재시도 뒤 건너뛴다.</li>
 *   <li>과거 궤적: 기록이 원본이므로 버리지 않고 무기한 재시도한다(장애 동안 consumer lag 만 쌓인다).</li>
 * </ul>
 */
@Configuration
public class KafkaConsumerConfig {

    public static final String REALTIME_FACTORY = "realtimeBatchFactory";
    public static final String HISTORY_FACTORY = "historyBatchFactory";
    public static final String INCIDENT_INDEXER_FACTORY = "incidentIndexerFactory";
    public static final String COMMAND_ACK_FACTORY = "commandAckFactory";

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean(REALTIME_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> realtimeBatchFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);

        // 선점 리스(기본 15초)가 풀릴 때까지 기다릴 수 있도록 최대 60초까지 재시도한다.
        ExponentialBackOff backOff = new ExponentialBackOff(200L, 2.0);
        backOff.setMaxInterval(5_000L);
        backOff.setMaxElapsedTime(60_000L);
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                (record, ex) -> log.error("실시간 라우팅 재시도 소진, 메시지를 건너뜁니다. topic={} partition={} offset={} key={}",
                        record.topic(), record.partition(), record.offset(), record.key(), ex),
                backOff));
        return factory;
    }

    /**
     * 차량 명령 응답(ACK) 소비용(레코드 단위).
     *
     * <p>Redis 연결 장애·명령 시간 초과처럼 복구될 수 있는 오류만 무기한 재시도한다. 재시도 동안 그 파티션의 ACK 처리는
     * 멈추지만 버리지 않으므로, Redis 가 돌아오면 실행된 명령이 ACKNOWLEDGED 로 반영된다(기본 핸들러는 즉시 10회 뒤 버린다).
     * 그 밖의 오류(손상된 기록 등)는 재시도해도 같으므로 바로 로그를 남기고 넘어가, 뒤따르는 ACK 처리를 막지 않는다.
     * 해석할 수 없는 메시지는 리스너가 직접 건너뛴다.
     */
    @Bean(COMMAND_ACK_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> commandAckFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(false);
        ExponentialBackOff backOff = new ExponentialBackOff(200L, 2.0);
        backOff.setMaxInterval(5_000L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, ex) -> log.error("명령 응답 처리 실패(재시도해도 같은 오류), 건너뜁니다 partition={} offset={} value={}",
                        record.partition(), record.offset(), record.value(), ex),
                backOff);
        errorHandler.defaultFalse();
        errorHandler.addRetryableExceptions(DataAccessResourceFailureException.class, TransientDataAccessException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * 돌발상황 이력 색인용. Elasticsearch 가 멈추면 결과를 버리지 않고 무기한 재시도한다(그동안 lag 만 쌓이고,
     * 복구되면 밀린 결과를 처리한다). 복구를 빨리 알아채도록 재시도 간격 상한을 5초로 둔다.
     */
    @Bean(INCIDENT_INDEXER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> incidentIndexerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(5_000L);
        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));
        return factory;
    }

    @Bean(HISTORY_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> historyBatchFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);

        // maxElapsedTime 기본값은 무제한이다. 재시도 중에는 컨테이너가 consumer 를 pause 하고 poll 을 계속해
        // max.poll.interval.ms 초과로 그룹에서 빠지지 않는다.
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(30_000L);
        factory.setCommonErrorHandler(new DefaultErrorHandler(backOff));
        return factory;
    }
}
