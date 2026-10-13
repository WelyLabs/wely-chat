package com.calendar.chat.domain.models;

import java.util.List;

/**
 * A message the application posted into a conversation.
 *
 * @param message        as stored, with its conversation set
 * @param participantIds who takes part, to deliver it to
 * @param duplicate      the same message was already there, and nothing was written
 */
public record PostedMessage(Message message, List<String> participantIds, boolean duplicate) {}
