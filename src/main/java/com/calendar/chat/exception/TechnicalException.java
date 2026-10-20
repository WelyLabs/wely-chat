package com.calendar.chat.exception;

import lombok.Getter;

/**
 * A dependency this service needs is unavailable.
 *
 * <p>Distinct from {@link BusinessException} because the two call for different responses:
 * a business failure means the caller should change the request, a technical one means the
 * caller should retry and someone should look at an incident.
 */
@Getter
public class TechnicalException extends RuntimeException {

    private final TechnicalErrorCode errorCode;

    public TechnicalException(TechnicalErrorCode errorCode) {
        super(errorCode.getDetail());
        this.errorCode = errorCode;
    }

    /**
     * Keeps the failure that made the dependency unavailable, so the ERROR log shows the real
     * cause (MongoDB, WebClient…). The response still carries only the code's detail.
     */
    public TechnicalException(TechnicalErrorCode errorCode, Throwable cause) {
        super(errorCode.getDetail(), cause);
        this.errorCode = errorCode;
    }
}
