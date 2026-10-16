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

    /** Like CONVERSATION_NOT_FOUND: an unknown call and someone else's call look the same. */
    CALL_NOT_FOUND(
            "CHT-BUS-008",
            "Call not found",
            "No live call matches the given identifier for this user.",
            HttpStatus.NOT_FOUND),

    CALL_FULL(
            "CHT-BUS-009",
            "Call full",
            "This call already has as many participants as it can take.",
            HttpStatus.CONFLICT),

    CALL_TRACK_DENIED(
            "CHT-BUS-010",
            "Track not available",
            "Only the tracks published by participants of this call can be received.",
            HttpStatus.FORBIDDEN),

    /** The caller's account is deleted: their token may still be valid for a few minutes, their writes are not. */
    ACCOUNT_DELETED(
            "CHT-BUS-011",
            "Account deleted",
            "This account has been deleted.",
            HttpStatus.GONE),

    /** A private conversation whose other participant deleted their account stays readable, not writable. */
    CONVERSATION_READ_ONLY(
            "CHT-BUS-012",
            "Conversation read-only",
            "The other participant deleted their account: this conversation can be read, not written to.",
            HttpStatus.CONFLICT),

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
