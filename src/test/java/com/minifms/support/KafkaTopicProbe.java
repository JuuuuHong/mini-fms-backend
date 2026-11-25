package com.minifms.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * 테스트용 토픽 관찰자. 고유 group 으로 처음부터 읽으므로 앞선 테스트의 메시지도 보인다 — 호출하는 쪽이
 * 자기 vehicleId·ID 로 걸러서 판단한다.
 */
public final class KafkaTopicProbe implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    public KafkaTopicProbe(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(topic));
    }

    /** 조건을 만족하거나 시간이 다 될 때까지 읽는다. 지금까지 받은 전체 메시지를 돌려준다. */
    public List<ConsumerRecord<String, String>> pollUntil(Predicate<List<ConsumerRecord<String, String>>> done,
                                                          Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(received::add);
            if (done.test(received)) {
                break;
            }
        }
        return List.copyOf(received);
    }

    /** 정해진 시간 동안 계속 읽는다(더 오지 않아야 하는 메시지를 확인할 때). */
    public List<ConsumerRecord<String, String>> pollFor(Duration duration) {
        return pollUntil(records -> false, duration);
    }

    @Override
    public void close() {
        consumer.close();
    }
}
