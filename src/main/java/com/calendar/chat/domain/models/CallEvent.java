package com.calendar.chat.domain.models;

/**
 * A call event addressed to one person.
 *
 * @param actorId who caused it: the caller, who joined, left, published or declined
 * @param call    the call as it stands after the event
 */
public record CallEvent(CallEventType type, String recipientId, String actorId, Call call) {}
