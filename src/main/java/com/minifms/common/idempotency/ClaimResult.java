package com.minifms.common.idempotency;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * 한 번의 선점 요청 결과. statuses 는 businessKeys 와 같은 순서·같은 길이다.
 *
 * @param token 이번 처리 시도를 구별하는 값. complete·release 는 이 토큰으로 선점한 키에만 적용된다.
 */
public record ClaimResult(String namespace, String token, List<String> businessKeys, List<ClaimStatus> statuses) {

    public ClaimResult {
        if (businessKeys.size() != statuses.size()) {
            throw new IllegalArgumentException("키와 결과 개수가 다릅니다");
        }
    }

    public ClaimStatus status(int index) {
        return statuses.get(index);
    }

    public boolean anyInFlight() {
        return statuses.contains(ClaimStatus.IN_FLIGHT);
    }

    /** 이번 시도가 선점한 키(중복 제거, 입력 순서 유지). */
    public List<String> claimedKeys() {
        LinkedHashSet<String> claimed = new LinkedHashSet<>();
        for (int i = 0; i < businessKeys.size(); i++) {
            if (statuses.get(i) == ClaimStatus.CLAIMED) {
                claimed.add(businessKeys.get(i));
            }
        }
        return new ArrayList<>(claimed);
    }

    public long count(ClaimStatus status) {
        return statuses.stream().filter(s -> s == status).count();
    }
}
