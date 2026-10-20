package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.exception.TechnicalErrorCode;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import static org.assertj.core.api.Assertions.assertThat;

class StoreErrorsTest {

    @Test
    void unavailable_shouldTranslateAMongoFailureIntoStoreUnavailable_withoutItsMessage() {
        var translated = StoreErrors.unavailable(new DataAccessResourceFailureException("mongodb://admin:secret@host"));

        assertThat(translated.getErrorCode()).isEqualTo(TechnicalErrorCode.STORE_UNAVAILABLE);
        assertThat(translated.getMessage()).doesNotContain("secret");
    }

    @Test
    void unavailable_shouldKeepTheMongoFailureAsTheCause() {
        var failure = new DataAccessResourceFailureException("connection refused");

        assertThat(StoreErrors.unavailable(failure).getCause()).isSameAs(failure);
    }
}
