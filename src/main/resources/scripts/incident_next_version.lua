-- incidentId 별 처리 결과 버전 발급(단조 증가)
-- KEYS[1] : 마지막 버전(fms:incident:{id}:result-version)
-- ARGV[1] : 현재 시각(ms)
-- ARGV[2] : 보존 기간(ms)
-- 반환    : 새 버전 = max(현재 시각, 마지막 버전 + 1)
--
-- 시각을 기본값으로 써서 Redis 키가 사라져도(만료·초기화) 버전이 과거로 돌아가지 않고,
-- 마지막 버전 + 1 로 같은 밀리초나 서버 간 시계 차이에도 앞선 결과보다 반드시 커진다.
local last = tonumber(redis.call('GET', KEYS[1]) or '0')
local version = tonumber(ARGV[1])
if version <= last then
  version = last + 1
end
redis.call('SET', KEYS[1], string.format('%.0f', version), 'PX', ARGV[2])
return version
