package com.calendar.chat.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** The conventions a client relies on when it branches on {@code code}, checked on every value. */
class BusinessErrorCodeTest {

    private static final String CODE_PATTERN = "^CHT-(BUS|VAL|REQ)-\\d{3}$";

    @Test
    void values_shouldFollowTheCodeConvention_forEveryCode() {
        assertThat(BusinessErrorCode.values())
                .allSatisfy(error -> assertThat(error.getCode()).as(error.name()).matches(CODE_PATTERN));
    }

    @Test
    void values_shouldBeUnique_acrossTheEnum() {
        assertThat(Arrays.stream(BusinessErrorCode.values()).map(BusinessErrorCode::getCode))
                .doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("a business failure is the caller's to fix: always a 4xx")
    void values_shouldAnswerA4xx_forEveryCode() {
        assertThat(BusinessErrorCode.values())
                .allSatisfy(error -> assertThat(error.getHttpStatus().is4xxClientError()).as(error.name()).isTrue());
    }

    @Test
    @DisplayName("no enum code collides with one the handler writes itself")
    void values_shouldNotReuseAHandlerCode_forAnyCode() {
        assertThat(Arrays.stream(BusinessErrorCode.values()).map(BusinessErrorCode::getCode))
                .doesNotContainAnyElementsOf(GlobalErrorHandler.HANDLER_CODES);
    }

    @Test
    void values_shouldNotOverlap_withTheTechnicalCodes() {
        assertThat(Arrays.stream(BusinessErrorCode.values()).map(BusinessErrorCode::getCode))
                .doesNotContainAnyElementsOf(Arrays.stream(TechnicalErrorCode.values())
                        .map(TechnicalErrorCode::getCode).toList());
    }
}
