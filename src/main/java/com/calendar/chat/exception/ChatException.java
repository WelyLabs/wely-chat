package com.calendar.chat.exception;

import lombok.Getter;

/**
 * A request the domain refuses on business grounds.
 *
 * <p>Carries no framework type, so the domain can raise it without depending on Spring.
 * {@code GlobalErrorHandler} turns it into an RFC 7807 response.
 */
@Getter
public class ChatException extends RuntimeException {

    private final ChatErrorCode errorCode;

    public ChatException(ChatErrorCode errorCode) {
        super(errorCode.getDetail());
        this.errorCode = errorCode;
    }
}
