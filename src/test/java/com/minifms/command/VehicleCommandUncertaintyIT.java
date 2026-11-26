package com.minifms.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.CommandType;
import com.minifms.command.domain.VehicleCommand;
import com.minifms.command.messaging.AckResult;
import com.minifms.command.messaging.VehicleCommandAck;
import com.minifms.common.config.properties.CommandProperties;
import com.minifms.common.config.properties.TopicProperties;
import com.minifms.common.kafka.KafkaEventPublisher;
import com.minifms.common.metrics.FmsMetrics;
import com.minifms.common.redis.RedisKeys;
import com.minifms.support.IntegrationTestSupport;
import com.minifms.support.KafkaTopicProbe;
import com.minifms.support.TestEvents;
import com.minifms.vehicle.location.VehicleLocationRepository;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * 결과가 불확실한 순간의 명령 처리.
 * <ul>
 *   <li>브로커는 명령을 받았지만 발행 확인이 늦게 오는 경우: 실패로 보상하지 않고 차량 ACK 로 끝나야 한다.</li>
 *   <li>성공 ACK 와 마감 처리가 동시에 오는 경우: 종료 상태·제어 상태·보상 이벤트가 한 번만, 서로 맞게 확정돼야 한다.</li>
 * </ul>
 * 테스트 프로필: 응답 마감 2초, 전송 확인 대기 1초, 차량 실행 여유 0.5초, 타임아웃 검사 0.2초 주기.
 */
class VehicleCommandUncertaintyIT extends IntegrationTestSupport {

    @Autowired
    VehicleCommandRepository commandRepository;
    @Autowired
    VehicleCommandService commandService;
    @Autowired
    VehicleLocationRepository locationRepository;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    TopicProperties topics;
    @Autowired
    CommandProperties commandProperties;
    @Autowired
    FmsMetrics metrics;
    @Autowired
    Clock clock;
    @Autowired
    StringRedisTemplate redis;

    @Test
    void 브로커는_명령을_받았지만_발행_확인이_늦으면_보상하지_않고_차량_ACK_로_완료된다() {
        String vehicleId = seedVehicle(TestEvents.uniqueId("SLOW"));
        // 실제로 브로커에 보내되(차량 TCU 가 받아 ACK 한다), 확인(future)은 전송 확인 대기(1초)보다 늦게 돌려준다.
        KafkaEventPublisher lateConfirm = new LateConfirmPublisher(kafkaTemplate, objectMapper, Duration.ofMillis(1_800));
        VehicleCommandService service = new VehicleCommandService(commandRepository, locationRepository, lateConfirm,
                objectMapper, topics, commandProperties, metrics, clock);
        double unknownBefore = metrics.count(FmsMetrics.COMMAND_TRANSMIT_UNKNOWN);

        VehicleCommand requested = service.request(vehicleId, CommandType.DOOR_LOCK);

        // 확인 시간 초과는 "실패"가 아니라 "모름"이다. 보상(FAILED·롤백)하지 않는다.
        assertThat(requested.status()).isNotEqualTo(CommandStatus.FAILED);
        assertThat(requested.rolledBack()).isFalse();
        assertThat(metrics.count(FmsMetrics.COMMAND_TRANSMIT_UNKNOWN) - unknownBefore).isEqualTo(1);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(commandRepository.find(requested.commandId()))
                        .map(VehicleCommand::status).hasValue(CommandStatus.ACKNOWLEDGED));
        VehicleCommand finished = commandRepository.find(requested.commandId()).orElseThrow();
        assertThat(finished.rolledBack()).isFalse();
        assertThat(commandRepository.findControlState(vehicleId).door()).isEqualTo("LOCKED");
        assertThat(redis.hasKey(RedisKeys.vehicleCommandLock(vehicleId))).isFalse();

        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.commandCompensations())) {
            assertThat(compensationsFor(probe.pollFor(Duration.ofSeconds(3)), List.of(requested.commandId())))
                    .as("보상 이벤트가 없어야 한다").isEmpty();
        }
    }

    @Test
    void 성공_ACK_와_마감_처리가_경합해도_종료_상태는_한_번만_확정되고_제어_상태와_보상_이벤트가_일치한다() throws Exception {
        int count = 30;
        long now = clock.millis();
        // 기준값은 명령을 만들기 전에 잰다. 만드는 동안에도 마감 스케줄러(0.2초 주기)가 이미 마감이 지난 명령을 TIMEOUT 으로
        // 확정할 수 있어, 만든 뒤에 재면 그 확정이 기준값에 들어가 증가분이 30 보다 작게 나온다.
        double lateAcksBefore = metrics.count(FmsMetrics.COMMAND_LATE_ACKS);
        double finishedBefore = finishedEngineStops();
        Map<String, String> vehicleOf = new HashMap<>();
        for (int i = 0; i < count; i++) {
            String vehicleId = seedVehicle(TestEvents.uniqueId("RACE"));
            String commandId = UUID.randomUUID().toString();
            // 이미 마감이 지난 대기 중 명령(TRANSMITTED 직전 상태)을 만든다.
            // 다른 테스트에서 늦게 끝나는 DOOR_* 명령과 지표가 섞이지 않도록 ENGINE_STOP 을 쓴다.
            commandRepository.create(commandId, vehicleId, CommandType.ENGINE_STOP, now - 5_000, now - 1,
                    Duration.ofSeconds(10), Duration.ofHours(1));
            vehicleOf.put(commandId, vehicleId);
        }

        // 명령마다 "성공 ACK 처리"와 "마감 처리"를 같은 순간에 출발시킨다(스케줄러도 0.2초마다 끼어든다).
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> tasks = new ArrayList<>();
        for (Map.Entry<String, String> entry : vehicleOf.entrySet()) {
            CountDownLatch start = new CountDownLatch(1);
            VehicleCommandAck ack = new VehicleCommandAck(entry.getKey(), entry.getValue(), AckResult.SUCCESS, null, now);
            tasks.add(pool.submit(() -> {
                start.await();
                commandService.handleAck(ack);
                return null;
            }));
            tasks.add(pool.submit(() -> {
                start.await();
                commandService.expireOverdue();
                return null;
            }));
            start.countDown();
        }
        for (Future<?> task : tasks) {
            task.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        List<String> timedOut = new ArrayList<>();
        for (Map.Entry<String, String> entry : vehicleOf.entrySet()) {
            VehicleCommand command = commandRepository.find(entry.getKey()).orElseThrow();
            String engine = commandRepository.findControlState(entry.getValue()).engine();
            assertThat(command.status()).isIn(CommandStatus.ACKNOWLEDGED, CommandStatus.TIMEOUT);
            if (command.status() == CommandStatus.TIMEOUT) {
                timedOut.add(entry.getKey());
                assertThat(command.rolledBack()).isTrue();
                assertThat(engine).as("TIMEOUT 이면 서버 기록을 이전 값으로 되돌린다").isEqualTo("ON");
            } else {
                assertThat(command.rolledBack()).isFalse();
                assertThat(engine).as("ACKNOWLEDGED 면 목표 값 유지").isEqualTo("OFF");
            }
            assertThat(redis.hasKey(RedisKeys.vehicleCommandLock(entry.getValue()))).as("잠금 해제").isFalse();
            assertThat(redis.opsForZSet().score(RedisKeys.COMMAND_DEADLINES, entry.getKey())).as("마감 제거").isNull();
        }
        System.out.printf("경합 결과: TIMEOUT %d건, ACKNOWLEDGED %d건%n", timedOut.size(), count - timedOut.size());
        // 명령마다 종료 확정(ACKNOWLEDGED 또는 TIMEOUT)은 정확히 한 번이다.
        assertThat(finishedEngineStops() - finishedBefore).as("종료 확정 횟수").isEqualTo(count);
        // TIMEOUT 이 이긴 명령은 그 뒤 도착한 성공 ACK 가 늦은 ACK 로 기록된다.
        assertThat(metrics.count(FmsMetrics.COMMAND_LATE_ACKS) - lateAcksBefore).isGreaterThanOrEqualTo(timedOut.size());

        // 보상 이벤트: TIMEOUT 인 명령마다 정확히 1건, ACKNOWLEDGED 인 명령은 0건.
        try (KafkaTopicProbe probe = new KafkaTopicProbe(bootstrapServers(), topics.commandCompensations())) {
            List<String> ids = new ArrayList<>(vehicleOf.keySet());
            probe.pollUntil(records -> compensationsFor(records, ids).size() >= timedOut.size(), Duration.ofSeconds(20));
            List<String> compensated = compensationsFor(probe.pollFor(Duration.ofSeconds(2)), ids);
            assertThat(compensated).containsExactlyInAnyOrderElementsOf(timedOut);
        }
    }

    private double finishedEngineStops() {
        return metrics.count(FmsMetrics.COMMANDS_FINISHED, "type", "ENGINE_STOP", "status", "ACKNOWLEDGED")
                + metrics.count(FmsMetrics.COMMANDS_FINISHED, "type", "ENGINE_STOP", "status", "TIMEOUT");
    }

    private List<String> compensationsFor(List<ConsumerRecord<String, String>> records, List<String> commandIds) {
        List<String> found = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            try {
                JsonNode event = objectMapper.readTree(record.value());
                String commandId = event.get("commandId").asText();
                if (commandIds.contains(commandId)) {
                    found.add(commandId);
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return found;
    }

    private String seedVehicle(String vehicleId) {
        long now = System.currentTimeMillis();
        locationRepository.updateIfNewer(TestEvents.northOf(vehicleId, 35.1595, 126.8526, 0, now), now);
        return vehicleId;
    }

    /** 브로커에는 실제로 보내지만(차량이 받는다), 확인은 delay 만큼 늦게 돌려준다(확인 응답 지연 재현). */
    static class LateConfirmPublisher extends KafkaEventPublisher {

        private final Duration delay;

        LateConfirmPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, Duration delay) {
            super(kafkaTemplate, objectMapper);
            this.delay = delay;
        }

        @Override
        public CompletableFuture<SendResult<String, String>> publish(String topic, String key, Object payload) {
            return super.publish(topic, key, payload).thenCompose(result -> CompletableFuture.supplyAsync(
                    () -> result, CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS)));
        }
    }
}
