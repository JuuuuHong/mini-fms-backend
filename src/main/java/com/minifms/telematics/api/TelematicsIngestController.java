package com.minifms.telematics.api;

import com.minifms.telematics.domain.TelematicsEvent;
import com.minifms.telematics.ingest.TelematicsIngestService;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/telematics")
public class TelematicsIngestController {

    private final TelematicsIngestService ingestService;

    public TelematicsIngestController(TelematicsIngestService ingestService) {
        this.ingestService = ingestService;
    }

    /** 단건 수집. Kafka 에 기록되면 202. */
    @PostMapping
    public ResponseEntity<IngestResponse> ingest(@Valid @RequestBody TelematicsEvent event) {
        return ResponseEntity.accepted().body(new IngestResponse(ingestService.ingest(List.of(event))));
    }

    /** 묶음 수집(단말이 버퍼에 모아 두었다가 한꺼번에 올리는 경우). 최대 1,000건. */
    @PostMapping("/batch")
    public ResponseEntity<IngestResponse> ingestBatch(@Valid @RequestBody TelematicsBatchRequest request) {
        return ResponseEntity.accepted().body(new IngestResponse(ingestService.ingest(request.events())));
    }
}
