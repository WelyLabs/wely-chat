package com.calendar.chat.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * Business outcomes this service refuses on, each with a stable code.
 *
 * <p>{@code code} is the contract a client branches on; {@code title} and {@code detail}
 * are English, for whoever reads the response or the logs.
 */
@Getter
@AllArgsConstructor
public enum ChatErrorCode {

    /**
     * Covers both "no such conversation" and "not yours", on purpose. Telling the two
     * apart would let anyone enumerate conversation ids and learn which exist.
     */
    CONVERSATION_NOT_FOUND(
            "CHT-BUS-001",
            "Conversation not found",
            "No conversation matches the given identifier for this user.",
            HttpStatus.NOT_FOUND),

    MESSAGE_PAGE_NOT_FOUND(
            "CHT-BUS-002",
            "History page not found",
            "No message page exists at the given index for this conversation.",
            HttpStatus.NOT_FOUND);

    private final String code;
    private final String title;
    private final String detail;
    private final HttpStatus httpStatus;
}
