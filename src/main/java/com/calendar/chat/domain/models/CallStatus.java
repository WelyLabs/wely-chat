package com.calendar.chat.domain.models;

/** Where a call stands. RINGING and ACTIVE are live; the others are final. */
public enum CallStatus {
    /** Started, nobody but the caller has joined yet. */
    RINGING,
    /** At least two people joined at some point. */
    ACTIVE,
    /** Everyone left after the call was answered. */
    ENDED,
    /** Nobody answered: the caller hung up, or the ringing timed out. */
    MISSED,
    /** The other person of a direct conversation refused it. */
    DECLINED;

    public boolean isLive() {
        return this == RINGING || this == ACTIVE;
    }
}
