-- 멱등키 일괄 선점
-- KEYS    : 멱등키 목록(같은 키가 여러 번 나올 수 있다)
-- ARGV[1] : 이번 처리 시도의 토큰
-- ARGV[2] : 선점 리스(ms)
-- 반환    : KEYS 와 같은 순서의 상태 목록(CLAIMED | DUPLICATE | IN_FLIGHT)
--
-- 같은 배치 안의 중복은 "이번 실행에서 이미 본 키"로 가린다. 값이 내 토큰인 것만으로 중복이라 하면, 연결이 끊겨
-- 클라이언트(Lettuce)가 이 스크립트를 다시 보냈을 때 자기 선점을 중복으로 오인해 메시지를 버리게 된다.
local result = {}
local seen = {}
for i, key in ipairs(KEYS) do
  if seen[key] then
    result[i] = 'DUPLICATE' -- 같은 배치 안에서 앞서 나온 키
  elseif redis.call('SET', key, ARGV[1], 'NX', 'PX', ARGV[2]) then
    result[i] = 'CLAIMED'
  else
    local current = redis.call('GET', key)
    if current == ARGV[1] then
      result[i] = 'CLAIMED'   -- 같은 시도의 재실행: 이미 내가 선점했다
    elseif current == 'DONE' then
      result[i] = 'DUPLICATE' -- 이미 처리 완료
    else
      result[i] = 'IN_FLIGHT'
    end
  end
  seen[key] = true
end
return result
