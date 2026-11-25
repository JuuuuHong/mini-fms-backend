-- 처리하지 못한 키의 선점 해제
-- KEYS    : 멱등키 목록
-- ARGV[1] : 선점에 쓴 토큰(자기 선점만 푼다)
-- 반환    : 푼 키 수
local released = 0
for _, key in ipairs(KEYS) do
  if redis.call('GET', key) == ARGV[1] then
    redis.call('DEL', key)
    released = released + 1
  end
end
return released
