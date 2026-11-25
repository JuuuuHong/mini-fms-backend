package com.minifms.incident;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class IncidentDispatchStatusTest {

    @Test
    void 대상_수와_발행_확인_건수로_처리_상태를_정한다() {
        assertThat(IncidentDispatchStatus.of(0, 0)).isEqualTo(IncidentDispatchStatus.NO_TARGETS);
        assertThat(IncidentDispatchStatus.of(3, 3)).isEqualTo(IncidentDispatchStatus.COMPLETED);
        assertThat(IncidentDispatchStatus.of(3, 2)).isEqualTo(IncidentDispatchStatus.PARTIALLY_FAILED);
        assertThat(IncidentDispatchStatus.of(3, 0)).isEqualTo(IncidentDispatchStatus.FAILED);
    }
}
