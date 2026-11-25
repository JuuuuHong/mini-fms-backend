package com.minifms.incident.api;

import com.minifms.incident.IncidentBroadcastService;
import com.minifms.incident.IncidentBroadcastService.BroadcastResult;
import com.minifms.incident.IncidentReport;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** RSU·도로 센서 게이트웨이가 돌발상황을 올리는 API. */
@RestController
@RequestMapping("/api/v1/incidents")
public class IncidentController {

    private final IncidentBroadcastService broadcastService;

    public IncidentController(IncidentBroadcastService broadcastService) {
        this.broadcastService = broadcastService;
    }

    @PostMapping
    public ResponseEntity<BroadcastResult> report(@Valid @RequestBody IncidentReport report) {
        return ResponseEntity.accepted().body(broadcastService.broadcast(report));
    }
}
