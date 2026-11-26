package com.minifms.common.lock;

/** 정해진 시간 안에 분산 락을 얻지 못함(같은 대상에 대한 다른 요청이 처리 중). */
public class LockUnavailableException extends RuntimeException {

    public LockUnavailableException(String key) {
        super("분산 락을 얻지 못했습니다: " + key);
    }
}
