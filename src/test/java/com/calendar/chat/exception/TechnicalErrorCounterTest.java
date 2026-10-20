package com.calendar.chat.exception;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TechnicalErrorCounterTest {

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final TechnicalErrorCounter counter = new TechnicalErrorCounter(meterRegistry);

    private double count(String code, String source) {
        var found = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, source).counter();
        return found == null ? 0 : found.count();
    }

    @Test
    void count_shouldCountATechnicalFailure_underItsCodeAndSource() {
        counter.count(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE), GlobalErrorHandler.SOURCE_KAFKA);
        counter.count(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE), GlobalErrorHandler.SOURCE_KAFKA);
        counter.count(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE), GlobalErrorHandler.SOURCE_JOB);

        assertThat(count("CHT-TEC-004", GlobalErrorHandler.SOURCE_KAFKA)).isEqualTo(2);
        assertThat(count("CHT-TEC-004", GlobalErrorHandler.SOURCE_JOB)).isEqualTo(1);
    }

    @Test
    void count_shouldCountAnythingElse_asUnexpected() {
        counter.count(new IllegalStateException("boom"), GlobalErrorHandler.SOURCE_RECONCILIATION);

        assertThat(count(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE, GlobalErrorHandler.SOURCE_RECONCILIATION))
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a refused request or record is not an incident: nothing is counted")
    void count_shouldNotCountABusinessFailure() {
        counter.count(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED), GlobalErrorHandler.SOURCE_KAFKA);

        assertThat(meterRegistry.getMeters()).isEmpty();
    }
}
