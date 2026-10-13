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
            HttpStatus.NOT_FOUND),

    /**
     * No presence for this user: unknown, or no conversation shared with the caller. One code
     * for both, so presence cannot be probed for arbitrary user ids.
     */
    PRESENCE_NOT_FOUND(
            "CHT-BUS-003",
            "Presence not available",
            "Presence is only shown for people you share a conversation with.",
            HttpStatus.NOT_FOUND),

    PHOTO_INVALID(
            "CHT-BUS-004",
            "Photo not accepted",
            "Send a JPEG, PNG, GIF or BMP picture of at most 8000 pixels per side.",
            HttpStatus.BAD_REQUEST),

    PHOTO_TOO_LARGE(
            "CHT-BUS-005",
            "Photo too large",
            "The photo must be smaller than 10 MB.",
            HttpStatus.CONTENT_TOO_LARGE),

    PHOTO_NOT_FOUND(
            "CHT-BUS-006",
            "Photo not found",
            "No photo of this conversation matches the given message.",
            HttpStatus.NOT_FOUND),

    PHOTO_REMOVAL_DENIED(
            "CHT-BUS-007",
            "Not yours to remove",
            "A photo is removed by whoever sent it, or in an event's group by its organizer.",
            HttpStatus.FORBIDDEN),

    MEDIA_UNAVAILABLE(
            "CHT-TEC-001",
            "Photo storage unavailable",
            "The photo could not be stored or removed; try again shortly.",
            HttpStatus.BAD_GATEWAY);

    private final String code;
    private final String title;
    private final String detail;
    private final HttpStatus httpStatus;
}
