package com.minifms.incident.api;

import com.minifms.incident.IncidentBroadcastService;
import com.minifms.incident.IncidentBroadcastService.BroadcastResult;
import com.minifms.incident.IncidentReport;
import com.minifms.incident.IncidentType;
import com.minifms.incident.history.IncidentHistoryService;
import com.minifms.incident.history.IncidentHistoryService.IncidentHistoryPage;
import com.minifms.incident.history.IncidentHistoryView;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 돌발상황 API.
 * <ul>
 *   <li>POST: RSU·도로 센서 게이트웨이가 보고(incident:write). 반경 안 주행 차량에 경고를 보낸다.</li>
 *   <li>GET: 관제 운영자가 과거 처리 이력을 검색(fleet:read). Elasticsearch 에서 읽고, 닿지 못하면 503.</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/v1/incidents")
public class IncidentController {

    private final IncidentBroadcastService broadcastService;
    private final IncidentHistoryService historyService;

    public IncidentController(IncidentBroadcastService broadcastService, IncidentHistoryService historyService) {
        this.broadcastService = broadcastService;
        this.historyService = historyService;
    }

    @PostMapping
    public ResponseEntity<BroadcastResult> report(@Valid @RequestBody IncidentReport report) {
        return ResponseEntity.accepted().body(broadcastService.broadcast(report));
    }

    /**
     * 발생 시각 [from, to) 범위의 이력을 최신 발생 순으로 페이지 조회한다(같은 시각이면 incidentId 오름차순).
     * 기간은 최대 31일, size 는 최대 100.
     */
    @GetMapping
    public IncidentHistoryPage search(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) IncidentType type,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) int size) {
        return historyService.search(from, to, type, page, size);
    }

    @GetMapping("/{incidentId}")
    public IncidentHistoryView get(@PathVariable @Pattern(regexp = "^[A-Za-z0-9_-]{1,64}$") String incidentId) {
        return historyService.get(incidentId);
    }
}
