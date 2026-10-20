package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;

/** Translates MongoDB failures into STORE_UNAVAILABLE (502), so none reaches the domain as a Spring type. */
@Slf4j
final class StoreErrors {

    private StoreErrors() {
    }

    static TechnicalException unavailable(DataAccessException error) {
        log.warn("MongoDB failure: {}", error.toString());
        return new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE, error);
    }
}
