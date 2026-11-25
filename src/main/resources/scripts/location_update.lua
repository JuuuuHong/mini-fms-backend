-- 최신 위치 갱신(더 최신 이벤트일 때만)
-- KEYS[1] : GEO 인덱스(fms:vehicles:geo)
-- KEYS[2] : 차량 최신 상태 해시(fms:vehicle:{id}:latest)
-- KEYS[3] : 마지막 수신 시각 sorted set(fms:vehicles:last-seen)
-- ARGV    : 1 vehicleId, 2 longitude, 3 latitude, 4 timestamp(ms), 5 speed, 6 batterySoc,
--           7 engineStatus, 8 ingestedAt(ms), 9 최신 상태 TTL(초)
-- 반환    : 1(반영) | 0(저장된 것이 같거나 더 최신이라 무시)
--
-- 비교와 쓰기를 한 스크립트에서 해, 늦게 도착한 과거 이벤트가 최신 위치를 덮어쓰는 일을 원자적으로 막는다.
local current = redis.call('HGET', KEYS[2], 'timestamp')
if current and tonumber(current) >= tonumber(ARGV[4]) then
  return 0
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
return 1
