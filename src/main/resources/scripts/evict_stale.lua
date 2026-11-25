-- 오래 수신이 없는 차량을 GEO 인덱스에서 뺀다
-- KEYS[1] : 마지막 수신 시각 sorted set
-- KEYS[2] : GEO 인덱스
-- ARGV[1] : 기준 시각(ms). 이보다 오래된 차량을 뺀다
-- ARGV[2] : 한 번에 뺄 최대 수
-- 반환    : 뺀 차량 수
-- 조회와 삭제를 한 스크립트에서 해, 그 사이 새 위치가 들어온 차량을 잘못 빼지 않는다.
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('ZREM', KEYS[2], id)
end
return #ids
