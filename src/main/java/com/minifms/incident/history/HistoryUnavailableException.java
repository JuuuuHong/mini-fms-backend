package com.minifms.incident.history;

/** 이력 저장소(Elasticsearch)에 닿을 수 없을 때. 503 으로 응답한다. */
public class HistoryUnavailableException extends RuntimeException {

    public HistoryUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
