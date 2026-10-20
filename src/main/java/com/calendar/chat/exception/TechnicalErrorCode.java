package com.calendar.chat.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * Failures of a dependency rather than of the request.
 *
 * <p>Each names which dependency broke, so an incident can be routed without reading the
 * logs. All of them answer 502 rather than 500: the request was valid and this service is
 * running — something it depends on is not, and 502 says exactly that.
 */
@Getter
@AllArgsConstructor
public enum TechnicalErrorCode {

    MEDIA_UNAVAILABLE(
            "CHT-TEC-001",
            "Photo storage unavailable",
            "The photo could not be stored or removed; try again shortly.",
            HttpStatus.BAD_GATEWAY),

    CALLS_UNAVAILABLE(
            "CHT-TEC-002",
            "Calls unavailable",
            "The call service could not be reached; try again shortly.",
            HttpStatus.BAD_GATEWAY),

    /** wely-users' list of deleted accounts could not be read; only the reconciliation loop reads it. */
    USERS_UNAVAILABLE(
            "CHT-TEC-003",
            "User directory unavailable",
            "The deleted accounts could not be listed from wely-users; try again shortly.",
            HttpStatus.BAD_GATEWAY),

    STORE_UNAVAILABLE(
            "CHT-TEC-004",
            "Message store unavailable",
            "The message store could not be reached; try again shortly.",
            HttpStatus.BAD_GATEWAY);

    private final String code;
    private final String title;
    private final String detail;
    private final HttpStatus httpStatus;
}
