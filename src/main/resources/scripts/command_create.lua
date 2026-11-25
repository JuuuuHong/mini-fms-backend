-- 원격 제어 명령 생성(사가 1단계: 로컬 트랜잭션)
-- KEYS[1] : 차량 명령 잠금(fms:vehicle:{id}:command-lock)
-- KEYS[2] : 차량 제어 상태 해시(fms:vehicle:{id}:shadow)
-- KEYS[3] : 명령 해시(fms:command:{commandId})
-- KEYS[4] : 응답 마감 sorted set(fms:command:deadlines)
-- ARGV    : 1 commandId, 2 vehicleId, 3 type, 4 제어 필드(door|engine), 5 목표 값, 6 필드 기본값,
--           7 requestedAt(ms), 8 deadlineAt(ms), 9 잠금 TTL(ms), 10 명령 기록 보존(ms)
-- 반환    : {1, 이전 값} | {0, 진행 중인 commandId}
--
-- 잠금 획득 → 이전 값 스냅샷 → 목표 값 선반영(낙관적) → 명령 기록 → 마감 등록을 원자적으로 한다.
-- 차량당 진행 중 명령을 하나로 제한해, 보상(롤백) 때 되돌릴 "이전 값"이 다른 명령에 의해 흐트러지지 않게 한다.
local holder = redis.call('GET', KEYS[1])
if holder then
  return {0, holder}
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[9])

local previous = redis.call('HGET', KEYS[2], ARGV[4])
if not previous then
  previous = ARGV[6]
end
redis.call('HSET', KEYS[2], ARGV[4], ARGV[5])

redis.call('HSET', KEYS[3],
  'commandId', ARGV[1],
  'vehicleId', ARGV[2],
  'type', ARGV[3],
  'status', 'REQUESTED',
  'field', ARGV[4],
  'previousValue', previous,
  'targetValue', ARGV[5],
  'requestedAt', ARGV[7],
  'deadlineAt', ARGV[8],
  'updatedAt', ARGV[7])
redis.call('PEXPIRE', KEYS[3], ARGV[10])
redis.call('ZADD', KEYS[4], ARGV[8], ARGV[1])
return {1, previous}
