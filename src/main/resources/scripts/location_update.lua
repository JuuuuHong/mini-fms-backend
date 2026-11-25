-- 최신 위치 갱신 + 지오펜스 전이 + 알림 아웃박스 적재(더 최신 이벤트일 때만, 한 원자 연산)
-- KEYS[1]     : GEO 인덱스(fms:vehicles:geo)
-- KEYS[2]     : 차량 최신 상태 해시(fms:vehicle:{id}:latest)
-- KEYS[3]     : 마지막 수신 시각 sorted set(fms:vehicles:last-seen)
-- KEYS[4]     : 아웃박스(fms:outbox)
-- KEYS[5..]   : 판정 대상 지오펜스의 "안에 있는 차량" 집합(fms:geofence:{id}:inside)
-- ARGV[1..9]  : vehicleId, longitude, latitude, timestamp(ms), speed, batterySoc, engineStatus,
--               ingestedAt(ms), 최신 상태 TTL(초)
-- ARGV[10..]  : 지오펜스마다 2개씩(KEYS[5..] 순서) — 전이 후보(ENTER|EXIT), 전이 시 적재할 알림(JSON)
-- 반환        : {0}(저장된 것이 같거나 더 최신이라 무시) | {1, 지오펜스별 변경 여부(1|0)...}
--
-- 위치 저장과 지오펜스 판정을 따로 하면, 위치만 저장된 뒤 실패했을 때 재전달 메시지는 "이미 반영된 시각"이라
-- 지오펜스 판정을 건너뛰어 알림을 잃는다. 한 스크립트에서 함께 하므로 둘 다 반영되거나 둘 다 반영되지 않는다.
local current = redis.call('HGET', KEYS[2], 'timestamp')
if current and tonumber(current) >= tonumber(ARGV[4]) then
  return {0}
end
redis.call('GEOADD', KEYS[1], ARGV[2], ARGV[3], ARGV[1])
redis.call('HSET', KEYS[2],
  'vehicleId', ARGV[1],
  'latitude', ARGV[3],
  'longitude', ARGV[2],
  'speed', ARGV[5],
  'batterySoc', ARGV[6],
  'engineStatus', ARGV[7],
  'timestamp', ARGV[4],
  'updatedAt', ARGV[8])
redis.call('EXPIRE', KEYS[2], ARGV[9])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])

local result = {1}
for i = 5, #KEYS do
  local base = 10 + (i - 5) * 2
  local changed
  if ARGV[base] == 'ENTER' then
    changed = redis.call('SADD', KEYS[i], ARGV[1])
  else
    changed = redis.call('SREM', KEYS[i], ARGV[1])
  end
  if changed == 1 then
    redis.call('RPUSH', KEYS[4], ARGV[base + 1])
  end
  result[#result + 1] = changed
end
return result
