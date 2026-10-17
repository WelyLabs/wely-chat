package com.calendar.chat.application.dtos;

import java.time.Instant;
import java.util.List;

/**
 * A call as the browser sees it. Relay session ids are left out: the browser never needs them,
 * the server looks them up itself.
 */
public record CallDTO(String id, String conversationId, String startedBy, String startedByName, String media,
                      String status,
                      Instant startedAt, Instant endedAt, List<String> memberIds, List<Participant> participants) {

    /** Someone in the call, and what they publish. */
    public record Participant(String userId, Instant joinedAt, List<String> trackNames) {}
}
