package com.calendar.chat.domain.models;

import java.time.Instant;
import java.util.List;

/**
 * Someone in a call.
 *
 * @param sessionId  their session on the media relay: one per browser connection
 * @param lastSeenAt their last heartbeat; a silent participant is dropped
 * @param trackNames what they publish, such as "audio" or "camera"
 * @param cameraOff  their camera is published but turned off: show their picture, not a frozen frame
 */
public record CallParticipant(String userId, String sessionId, Instant joinedAt, Instant lastSeenAt,
                              List<String> trackNames, boolean cameraOff) {

    public CallParticipant {
        trackNames = List.copyOf(trackNames);
    }

    public boolean publishes(String trackName) {
        return trackNames.contains(trackName);
    }
}
