-- 돌발상황 처리 결과 기록: 선점 확인 + 버전 발급 + 아웃박스 적재(원자적)
-- KEYS[1] : 이 돌발상황의 멱등키(fms:dedup:incident:{id})
-- KEYS[2] : 마지막 결과 버전(fms:incident:{id}:result-version)
-- KEYS[3] : 아웃박스(fms:outbox)
-- ARGV[1] : 이 처리의 선점 토큰
-- ARGV[2] : 현재 시각(ms)
-- ARGV[3] : 버전 보존 기간(ms)
-- ARGV[4] : 아웃박스 항목(JSON). 버전 자리는 "version":-1 로 비워 둔다
-- 반환    : 새 버전 | -1(선점을 잃음 — 기록하지 않는다)
--
-- 선점이 만료돼 다른 처리(RSU 재시도)가 이어받은 뒤에 이전 처리가 늦게 끝나 결과를 남기면, 더 큰 버전을 받아
-- 이어받은 처리의 결과를 덮어쓴다. 선점을 아직 가지고 있을 때만 버전을 발급하고 적재한다.
-- 버전 = max(현재 시각, 마지막 버전 + 1): Redis 가 발급 순서를 직렬화하고, 키가 사라져도 과거로 돌아가지 않는다.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return -1
end
local last = tonumber(redis.call('GET', KEYS[2]) or '0')
local version = tonumber(ARGV[2])
if version <= last then
  version = last + 1
end
local text = string.format('%.0f', version)
local entry, replaced = string.gsub(ARGV[4], '"version":%-1', '"version":' .. text, 1)
if replaced ~= 1 then
  return redis.error_reply('outbox entry has no version placeholder')
end
redis.call('SET', KEYS[2], text, 'PX', ARGV[3])
redis.call('RPUSH', KEYS[3], entry)
return version
