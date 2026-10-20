package com.calendar.chat.exception;

import lombok.Getter;

/**
 * A request the domain refuses on business grounds.
 *
 * <p>Carries no framework type, so the domain can raise it without depending on Spring.
 * {@code GlobalErrorHandler} turns it into an RFC 7807 response.
 */
@Getter
public class BusinessException extends RuntimeException {

    private final BusinessErrorCode errorCode;

    public BusinessException(BusinessErrorCode errorCode) {
        super(errorCode.getDetail());
        this.errorCode = errorCode;
    }
}
