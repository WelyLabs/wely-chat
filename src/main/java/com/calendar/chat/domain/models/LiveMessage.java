package com.calendar.chat.domain.models;

/** A message pushed to one recipient's stream, and whether it is new or replaces one already shown. */
public record LiveMessage(Message message, BroadcastKind kind) {}
