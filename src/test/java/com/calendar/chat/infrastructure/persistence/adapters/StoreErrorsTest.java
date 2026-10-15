package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.exception.ChatErrorCode;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import static org.assertj.core.api.Assertions.assertThat;

class StoreErrorsTest {

    @Test
    void unavailable_shouldTranslateAMongoFailureIntoStoreUnavailable_withoutItsMessage() {
        var translated = StoreErrors.unavailable(new DataAccessResourceFailureException("mongodb://admin:secret@host"));

        assertThat(translated.getErrorCode()).isEqualTo(ChatErrorCode.STORE_UNAVAILABLE);
        assertThat(translated.getMessage()).doesNotContain("secret");
    }
}
