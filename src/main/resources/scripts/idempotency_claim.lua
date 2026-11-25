-- 멱등키 일괄 선점
-- KEYS    : 멱등키 목록(같은 키가 여러 번 나올 수 있다)
-- ARGV[1] : 이번 처리 시도의 토큰
-- ARGV[2] : 선점 리스(ms)
-- 반환    : KEYS 와 같은 순서의 상태 목록(CLAIMED | DUPLICATE | IN_FLIGHT)
local result = {}
for i, key in ipairs(KEYS) do
  if redis.call('SET', key, ARGV[1], 'NX', 'PX', ARGV[2]) then
    result[i] = 'CLAIMED'
  else
    local current = redis.call('GET', key)
    if current == 'DONE' or current == ARGV[1] then
      -- 이미 처리 완료, 또는 같은 배치 안에서 앞서 이 토큰으로 선점한 키
      result[i] = 'DUPLICATE'
    else
      result[i] = 'IN_FLIGHT'
    end
  end
end
return result
