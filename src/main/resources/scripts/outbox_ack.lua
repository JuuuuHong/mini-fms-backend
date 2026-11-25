-- 발행 확인된 아웃박스 항목 삭제(잠금 소유자 + 목록 앞부분 일치를 원자적으로 확인)
-- KEYS[1] : 릴레이 잠금
-- KEYS[2] : 아웃박스
-- ARGV[1] : 릴레이 소유자 ID
-- ARGV[2..] : 발행 확인된 항목들의 SHA-1(읽은 순서)
-- 반환    : 지운 항목 수 | -1(잠금을 잃음, 지우지 않음)
--
-- "읽은 건수만큼 LTRIM" 만 하면, 발행을 기다리는 사이 잠금이 만료돼 다른 릴레이가 같은 항목을 처리·삭제한 경우
-- 그다음에 있는 미발행 항목을 지운다. 지금 목록의 앞부분이 내가 발행한 항목과 같을 때만, 같은 만큼만 지운다.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return -1
end
local expected = #ARGV - 1
local head = redis.call('LRANGE', KEYS[2], 0, expected - 1)
local matched = 0
for i = 1, #head do
  if redis.sha1hex(head[i]) == ARGV[i + 1] then
    matched = matched + 1
  else
    break
  end
end
if matched > 0 then
  redis.call('LTRIM', KEYS[2], matched, -1)
end
return matched
