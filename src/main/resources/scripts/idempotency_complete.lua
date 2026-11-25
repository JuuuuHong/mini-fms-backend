-- 선점한 키를 처리 완료(DONE)로 확정
-- KEYS    : 멱등키 목록
-- ARGV[1] : 선점에 쓴 토큰(자기 선점만 확정한다)
-- ARGV[2] : 중복 판별 윈도우(ms)
-- 반환    : 확정한 키 수
local done = 0
for _, key in ipairs(KEYS) do
  if redis.call('GET', key) == ARGV[1] then
    redis.call('SET', key, 'DONE', 'PX', ARGV[2])
    done = done + 1
  end
end
return done
