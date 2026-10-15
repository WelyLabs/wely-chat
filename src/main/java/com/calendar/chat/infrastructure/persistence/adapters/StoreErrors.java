package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;

/** Translates MongoDB failures into STORE_UNAVAILABLE (502), so none reaches the domain as a Spring type. */
@Slf4j
final class StoreErrors {

    private StoreErrors() {
    }

    static ChatException unavailable(DataAccessException error) {
        log.warn("MongoDB failure: {}", error.toString());
        return new ChatException(ChatErrorCode.STORE_UNAVAILABLE);
    }
}
