package com.minifms.common.idempotency;

/** 멱등키 선점 결과. */
public enum ClaimStatus {
    /** 이번 처리 시도가 선점했다. 처리 후 complete, 실패하면 release 해야 한다. */
    CLAIMED,
    /** 이미 처리가 끝났거나(DONE) 같은 배치 안에서 앞서 나온 메시지다. 버린다. */
    DUPLICATE,
    /**
     * 다른 처리 시도가 선점 중이다(리밸런스 직후, 또는 앞선 시도가 죽고 리스가 남은 경우).
     * 버리면 앞선 시도가 끝내 실패했을 때 메시지를 잃으므로, 리스가 풀릴 때까지 재시도한다.
     */
    IN_FLIGHT
}
