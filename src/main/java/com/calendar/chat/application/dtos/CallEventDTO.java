package com.calendar.chat.application.dtos;

/** A frame of {@code call.events}: what happened, who did it, and the call as it now stands. */
public record CallEventDTO(String type, String actorId, CallDTO call) {}
