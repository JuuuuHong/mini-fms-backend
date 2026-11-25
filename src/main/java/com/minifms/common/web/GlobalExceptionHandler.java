package com.minifms.common.web;

import com.minifms.common.kafka.EventPublishException;
import com.minifms.incident.history.HistoryUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * 오류 응답은 RFC 9457 Problem Details 형식으로 통일한다.
 * 요청 본문·파라미터 검증 오류(400)는 상위 클래스가 처리한다.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ProblemDetail> handleNotFound(NotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(ConflictException.class)
    public ResponseEntity<ProblemDetail> handleConflict(ConflictException e) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        e.properties().forEach(detail::setProperty);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(detail);
    }

    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<ProblemDetail> handleInvalid(InvalidRequestException e) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        detail.setProperty("violations", e.violations());
        return ResponseEntity.badRequest().body(detail);
    }

    @ExceptionHandler(EventPublishException.class)
    public ResponseEntity<ProblemDetail> handlePublishFailure(EventPublishException e) {
        log.error("Kafka 발행 실패", e);
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "메시지 브로커에 기록하지 못했습니다. 잠시 뒤 다시 시도하세요.");
    }

    @ExceptionHandler(HistoryUnavailableException.class)
    public ResponseEntity<ProblemDetail> handleHistoryUnavailable(HistoryUnavailableException e) {
        log.warn("관제 이력 저장소 장애: {}", e.getCause() == null ? e.getMessage() : e.getCause().toString());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage()));
    }

    /** Redis·ClickHouse 연결 실패·시간 초과는 잠시 뒤 다시 시도하면 되는 오류다(503). */
    @ExceptionHandler({DataAccessResourceFailureException.class, TransientDataAccessException.class})
    public ResponseEntity<ProblemDetail> handleStoreUnavailable(DataAccessException e) {
        log.warn("저장소 일시 장애: {}", e.toString());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                        "저장소에 일시적으로 연결할 수 없습니다. 잠시 뒤 다시 시도하세요."));
    }

    /** 처리하지 못한 예외도 같은 형식(Problem Details)으로 돌려준다. 내부 메시지는 응답에 싣지 않는다. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetail> handleUnexpected(Exception e) {
        log.error("처리하지 못한 예외", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "서버 오류가 발생했습니다.");
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(ProblemDetail.forStatusAndDetail(status, message));
    }
}
