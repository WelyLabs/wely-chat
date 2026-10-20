package com.calendar.chat.exception;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * The one place that counts technical failures in {@value GlobalErrorHandler#TECHNICAL_ERRORS_METRIC},
 * tagged by {@code code} and by {@code source} — where it surfaced: an HTTP answer, a Kafka
 * consumer, the reconciliation loop, a scheduled job, an RSocket route.
 *
 * <p>A {@link TechnicalException} counts under its own code; a {@link BusinessException} is not
 * counted at all — a refused request or record is not an incident; anything else counts as
 * {@value GlobalErrorHandler#UNEXPECTED_FAILURE_CODE}. Domain services never count: they do not
 * know Micrometer, so the infrastructure that calls them does.
 */
@Component
public class TechnicalErrorCounter {

    private final MeterRegistry meterRegistry;

    public TechnicalErrorCounter(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void count(Throwable error, String source) {
        String code = switch (error) {
            case BusinessException ignored -> null;
            case TechnicalException technical -> technical.getErrorCode().getCode();
            default -> GlobalErrorHandler.UNEXPECTED_FAILURE_CODE;
        };
        if (code != null) {
            meterRegistry.counter(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC,
                    GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, source).increment();
        }
    }
}
