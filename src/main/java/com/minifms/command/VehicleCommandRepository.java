package com.minifms.command;

import com.minifms.command.domain.CommandStatus;
import com.minifms.command.domain.CommandType;
import com.minifms.command.domain.ControlField;
import com.minifms.command.domain.ControlState;
import com.minifms.command.domain.VehicleCommand;
import com.minifms.common.redis.RedisKeys;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;

/** 명령 상태·차량 제어 상태 저장소(Redis). 상태 변경은 모두 Lua 로 원자적으로 한다. */
@Repository
public class VehicleCommandRepository {

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> CREATE =
            RedisScript.of(new ClassPathResource("scripts/command_create.lua"), List.class);
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> TRANSITION =
            RedisScript.of(new ClassPathResource("scripts/command_transition.lua"), List.class);

    private final StringRedisTemplate redis;

    public VehicleCommandRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** 잠금을 얻으면 명령을 만들고 목표 값을 선반영한다. 다른 명령이 진행 중이면 created=false. */
    public CreateResult create(String commandId, String vehicleId, CommandType type, long requestedAt,
                               long deadlineAt, Duration lockTtl, Duration retention) {
        List<?> result = redis.execute(CREATE,
                List.of(RedisKeys.vehicleCommandLock(vehicleId), RedisKeys.vehicleShadow(vehicleId),
                        RedisKeys.command(commandId), RedisKeys.COMMAND_DEADLINES),
                commandId, vehicleId, type.name(), type.field().hashField(), type.targetValue(),
                type.field().defaultValue(), Long.toString(requestedAt), Long.toString(deadlineAt),
                Long.toString(lockTtl.toMillis()), Long.toString(retention.toMillis()));
        if (result == null || result.size() != 2) {
            throw new IllegalStateException("명령 생성 결과가 올바르지 않습니다: " + result);
        }
        return new CreateResult(((Number) result.get(0)).longValue() == 1L, String.valueOf(result.get(1)));
    }

    /**
     * 현재 상태가 from 중 하나일 때만 to 로 바꾼다(compare-and-set).
     *
     * @param terminal              종료 상태면 마감 제거·차량 잠금 해제
     * @param rollback              제어 상태를 명령 전 값으로 되돌림
     * @param requireDeadlinePassed 마감이 지났을 때만 전이(타임아웃 판정용)
     * @param outboxEntry           전이에 성공하면 아웃박스에 넣을 이벤트(없으면 null)
     */
    public TransitionResult transition(String commandId, String vehicleId, CommandStatus to,
                                       Set<CommandStatus> from, long now, boolean terminal, boolean rollback,
                                       String reason, boolean requireDeadlinePassed, String outboxEntry) {
        String allowed = from.stream().map(Enum::name).collect(Collectors.joining(","));
        List<?> result = redis.execute(TRANSITION,
                List.of(RedisKeys.command(commandId), RedisKeys.COMMAND_DEADLINES, RedisKeys.vehicleShadow(vehicleId),
                        RedisKeys.vehicleCommandLock(vehicleId), RedisKeys.OUTBOX),
                commandId, to.name(), allowed, Long.toString(now), flag(terminal), flag(rollback),
                reason == null ? "" : reason, flag(requireDeadlinePassed), outboxEntry == null ? "" : outboxEntry);
        if (result == null || result.size() != 2) {
            throw new IllegalStateException("명령 상태 전이 결과가 올바르지 않습니다: " + result);
        }
        String status = String.valueOf(result.get(1));
        return new TransitionResult(((Number) result.get(0)).longValue() == 1L,
                status.isEmpty() ? null : CommandStatus.valueOf(status));
    }

    public Optional<VehicleCommand> find(String commandId) {
        Map<String, String> hash = redis.<String, String>opsForHash().entries(RedisKeys.command(commandId));
        return hash.isEmpty() ? Optional.empty() : Optional.of(VehicleCommand.fromHash(hash));
    }

    /** 마감이 지난 명령 ID(오래된 순). */
    public List<String> findOverdueIds(long now, int limit) {
        Set<String> ids = redis.opsForZSet().rangeByScore(RedisKeys.COMMAND_DEADLINES, Double.NEGATIVE_INFINITY, now, 0, limit);
        return ids == null ? List.of() : List.copyOf(ids);
    }

    public void removeDeadline(String commandId) {
        redis.opsForZSet().remove(RedisKeys.COMMAND_DEADLINES, commandId);
    }

    public ControlState findControlState(String vehicleId) {
        Map<String, String> shadow = redis.<String, String>opsForHash().entries(RedisKeys.vehicleShadow(vehicleId));
        return new ControlState(
                shadow.getOrDefault(ControlField.DOOR.hashField(), ControlField.DOOR.defaultValue()),
                shadow.getOrDefault(ControlField.ENGINE.hashField(), ControlField.ENGINE.defaultValue()));
    }

    private static String flag(boolean value) {
        return value ? "1" : "0";
    }

    /** @param detail created 면 이전 값, 아니면 진행 중인 commandId */
    public record CreateResult(boolean created, String detail) {
    }

    /** @param status applied 면 전이 전 상태, 아니면 현재 상태(명령이 없으면 null) */
    public record TransitionResult(boolean applied, CommandStatus status) {
    }
}
