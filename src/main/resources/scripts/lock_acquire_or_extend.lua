-- 소유자 토큰 기반 잠금 획득·연장
-- KEYS[1] : 잠금 키
-- ARGV[1] : 소유자 ID
-- ARGV[2] : 잠금 TTL(ms)
-- 반환    : 1(획득 또는 연장) | 0(다른 소유자가 보유 중)
local holder = redis.call('GET', KEYS[1])
if holder == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if not holder then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
