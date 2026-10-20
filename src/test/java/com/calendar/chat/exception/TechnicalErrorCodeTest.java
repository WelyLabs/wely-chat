package com.calendar.chat.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** The conventions an incident is routed on, checked on every value. */
class TechnicalErrorCodeTest {

    private static final String CODE_PATTERN = "^CHT-TEC-\\d{3}$";

    @Test
    void values_shouldFollowTheCodeConvention_forEveryCode() {
        assertThat(TechnicalErrorCode.values())
                .allSatisfy(error -> assertThat(error.getCode()).as(error.name()).matches(CODE_PATTERN));
    }

    @Test
    void values_shouldBeUnique_acrossTheEnum() {
        assertThat(Arrays.stream(TechnicalErrorCode.values()).map(TechnicalErrorCode::getCode))
                .doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("a dependency failure is never the caller's fault: always a 5xx")
    void values_shouldAnswerA5xx_forEveryCode() {
        assertThat(TechnicalErrorCode.values())
                .allSatisfy(error -> assertThat(error.getHttpStatus().is5xxServerError()).as(error.name()).isTrue());
    }

    @Test
    @DisplayName("no enum code collides with one the handler writes itself, CHT-TEC-000 included")
    void values_shouldNotReuseAHandlerCode_forAnyCode() {
        assertThat(Arrays.stream(TechnicalErrorCode.values()).map(TechnicalErrorCode::getCode))
                .doesNotContainAnyElementsOf(GlobalErrorHandler.HANDLER_CODES);
    }

    @Test
    void values_shouldNotOverlap_withTheBusinessCodes() {
        assertThat(Arrays.stream(TechnicalErrorCode.values()).map(TechnicalErrorCode::getCode))
                .doesNotContainAnyElementsOf(Arrays.stream(BusinessErrorCode.values())
                        .map(BusinessErrorCode::getCode).toList());
    }
}
