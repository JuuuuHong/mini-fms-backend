package com.minifms.ride;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.minifms.common.web.GlobalExceptionHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;

/**
 * 라이드 DB 트랜잭션이 커밋 중에 연결을 잃으면 503(Retry-After)이어야 한다. 500 이 나가면 커밋됐는지 모르는 대여·반납에
 * 클라이언트가 재시도하지 않는다.
 */
class RideTransactionFailureIT extends RideTestSupport {

    @Autowired
    TransactionTemplate rideTransactionTemplate;
    @Autowired
    GlobalExceptionHandler exceptionHandler;

    @Test
    void 커밋_중_DB_연결이_끊기면_503_과_Retry_After() throws Exception {
        Throwable failure = catchThrowable(() -> rideTransactionTemplate.executeWithoutResult(status -> {
            // 트랜잭션에 묶인 연결의 백엔드를 다른 스레드(다른 연결)에서 끊고, 끊긴 것을 확인한 뒤 커밋한다.
            int pid = rideJdbcClient.sql("SELECT pg_backend_pid()").query(Integer.class).single();
            CompletableFuture.runAsync(() -> {
                rideJdbcClient.sql("SELECT pg_terminate_backend(:pid)").param("pid", pid).query(Boolean.class).single();
                await().atMost(Duration.ofSeconds(5)).until(() -> rideJdbcClient
                        .sql("SELECT count(*) FROM pg_stat_activity WHERE pid = :pid").param("pid", pid)
                        .query(Long.class).single() == 0);
            }).join();
        }));
        assertThat(failure).as("커밋 실패").isNotNull();

        // 컨트롤러에서 이 예외가 나면 받을 응답: Spring MVC 와 같은 규칙으로 GlobalExceptionHandler 의 처리기를 골라 부른다.
        Method handler = new ExceptionHandlerMethodResolver(GlobalExceptionHandler.class).resolveMethodByThrowable(failure);
        ResponseEntity<?> response = (ResponseEntity<?>) handler.invoke(exceptionHandler, failure);
        assertThat(response.getStatusCode()).as(failure.toString()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isEqualTo("5");

        // 끊긴 연결은 풀에서 빠지고 다음 트랜잭션은 정상이다.
        Integer one = rideTransactionTemplate.execute(status -> rideJdbcClient.sql("SELECT 1").query(Integer.class).single());
        assertThat(one).isEqualTo(1);
    }
}
