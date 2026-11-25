-- 명령 상태 전이(compare-and-set) + 선택적 롤백·보상 이벤트 적재
-- KEYS[1] : 명령 해시
-- KEYS[2] : 응답 마감 sorted set
-- KEYS[3] : 차량 제어 상태 해시
-- KEYS[4] : 차량 명령 잠금
-- KEYS[5] : 아웃박스
-- ARGV    : 1 commandId, 2 목표 상태, 3 허용하는 현재 상태(쉼표 구분), 4 now(ms),
--           5 종료 상태 여부('1'|'0'), 6 롤백 여부('1'|'0'), 7 사유(없으면 ''),
--           8 마감 경과 필수 여부('1'|'0'), 9 아웃박스 항목(없으면 '')
-- 반환    : {1, 이전 상태} | {0, 현재 상태(없으면 '')}
--
-- ACK 와 타임아웃이 동시에 와도 현재 상태 비교 덕분에 하나만 이긴다.
-- 롤백(제어 상태 되돌리기)·잠금 해제·보상 이벤트 적재가 상태 전이와 한 원자 연산이라, 중간에 죽어도 어긋나지 않는다.
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  return {0, ''}
end

local allowed = false
for s in string.gmatch(ARGV[3], '([^,]+)') do
  if s == status then
    allowed = true
  end
end
if not allowed then
  return {0, status}
end

if ARGV[8] == '1' then
  local deadline = tonumber(redis.call('HGET', KEYS[1], 'deadlineAt'))
  if deadline == nil or deadline > tonumber(ARGV[4]) then
    return {0, status}
  end
end

redis.call('HSET', KEYS[1], 'status', ARGV[2], 'updatedAt', ARGV[4])
if ARGV[2] == 'TRANSMITTED' then
  redis.call('HSET', KEYS[1], 'transmittedAt', ARGV[4])
end
if ARGV[7] ~= '' then
  redis.call('HSET', KEYS[1], 'failureReason', ARGV[7])
end

if ARGV[5] == '1' then
  redis.call('HSET', KEYS[1], 'completedAt', ARGV[4])
  redis.call('ZREM', KEYS[2], ARGV[1])
  if redis.call('GET', KEYS[4]) == ARGV[1] then
    redis.call('DEL', KEYS[4])
  end
end

if ARGV[6] == '1' then
  local field = redis.call('HGET', KEYS[1], 'field')
  local target = redis.call('HGET', KEYS[1], 'targetValue')
  local previous = redis.call('HGET', KEYS[1], 'previousValue')
  -- 이 명령이 선반영한 값이 그대로일 때만 되돌린다(다른 경로가 이미 바꿨다면 덮지 않는다).
  if redis.call('HGET', KEYS[3], field) == target then
    redis.call('HSET', KEYS[3], field, previous)
  end
  redis.call('HSET', KEYS[1], 'rolledBack', '1')
end

if ARGV[9] ~= '' then
  redis.call('RPUSH', KEYS[5], ARGV[9])
end
return {1, status}
