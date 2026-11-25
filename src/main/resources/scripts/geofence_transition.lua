-- 지오펜스 상태 전이 + 알림 아웃박스 적재(원자적)
-- KEYS[1] : 지오펜스 안 차량 집합(fms:geofence:{id}:inside)
-- KEYS[2] : 아웃박스(fms:outbox)
-- ARGV[1] : vehicleId
-- ARGV[2] : ENTER | EXIT
-- ARGV[3] : 전이가 일어났을 때 아웃박스에 넣을 알림(JSON)
-- 반환    : 1(상태가 바뀌어 알림 적재) | 0(이미 그 상태라 변화 없음)
--
-- SADD/SREM 의 반환값(실제로 바뀐 원소 수)으로 "이전 상태와 다른가"를 판단하므로 별도 조회가 필요 없다.
local changed
if ARGV[2] == 'ENTER' then
  changed = redis.call('SADD', KEYS[1], ARGV[1])
else
  changed = redis.call('SREM', KEYS[1], ARGV[1])
end
if changed == 1 then
  redis.call('RPUSH', KEYS[2], ARGV[3])
end
return changed
