package com.minifms.common.lock;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.stereotype.Component;

/**
 * Redisson 분산 락(RLock).
 *
 * <p>임대 시간을 주지 않아 Redisson watchdog 이 보유 중 락을 연장한다(작업이 길어져도 도중에 풀리지 않는다).
 * 프로세스가 죽으면 연장이 멈춰 lockWatchdogTimeout(기본 30초) 뒤 풀린다.
 *
 * <p><b>락은 정합성의 마지막 방어선이 아니다.</b> 긴 GC 정지·Redis 장애 조치로 락이 풀린 사이 다른 인스턴스가 같은 락을
 * 얻을 수 있다. 락은 경합을 줄이고 같은 기기에 대한 판정·외부 호출이 겹치지 않게 하는 용도이고, 상태 전이는 DB 의 조건부
 * 갱신(compare-and-set)과 유니크 인덱스가 최종적으로 막는다.
 */
@Component
public class DistributedLock {

    private static final Logger log = LoggerFactory.getLogger(DistributedLock.class);

    private final RedissonClient redisson;

    public DistributedLock(RedissonClient redisson) {
        this.redisson = redisson;
    }

    /**
     * wait 동안 락을 기다려 얻으면 action 을 실행하고 푼다.
     *
     * @throws LockUnavailableException 시간 안에 얻지 못함
     * @throws DataAccessResourceFailureException 락 저장소(Redis)에 연결할 수 없음(→ 503)
     */
    public <T> T executeWithLock(String key, Duration wait, Supplier<T> action) {
        RLock lock = redisson.getLock(key);
        if (!acquire(lock, key, wait)) {
            throw new LockUnavailableException(key);
        }
        try {
            return action.get();
        } finally {
            release(lock, key);
        }
    }

    /** 락을 바로 얻을 수 있을 때만 실행한다(주기 작업을 한 인스턴스만 돌릴 때). 얻지 못하면 empty. */
    public <T> Optional<T> tryExecute(String key, Supplier<T> action) {
        RLock lock = redisson.getLock(key);
        if (!acquire(lock, key, Duration.ZERO)) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(action.get());
        } finally {
            release(lock, key);
        }
    }

    private static boolean acquire(RLock lock, String key, Duration wait) {
        try {
            return lock.tryLock(wait.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LockUnavailableException(key);
        } catch (RedisException e) {
            // 서버에서는 잡혔는데 응답만 시간 초과됐을 수 있다. 그대로 두면 watchdog 없이 30초 동안 남아 그 대상의 요청이 막힌다.
            releaseIfHeld(lock, key);
            throw new DataAccessResourceFailureException("분산 락 저장소(Redis)에 연결할 수 없습니다: " + key, e);
        }
    }

    private static void releaseIfHeld(RLock lock, String key) {
        try {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        } catch (RuntimeException e) {
            log.warn("락 획득 실패 뒤 정리도 실패했습니다(최대 lockWatchdogTimeout 뒤 만료) key={}: {}", key, e.toString());
        }
    }

    /**
     * 락을 푼다. 보유 중 연장이 실패해 이미 풀렸다면(Redis 장애 등) unlock 이 실패하는데, 작업 결과는 DB 가 지키므로
     * 경고만 남긴다.
     */
    private static void release(RLock lock, String key) {
        try {
            lock.unlock();
        } catch (IllegalMonitorStateException e) {
            log.warn("작업 중 분산 락을 잃었습니다(이미 풀림) key={}", key);
        } catch (RuntimeException e) {
            log.warn("분산 락 해제 실패(watchdog 이 멈춰 곧 만료됨) key={}: {}", key, e.toString());
        }
    }
}
