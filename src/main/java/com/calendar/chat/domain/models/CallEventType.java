package com.calendar.chat.domain.models;

/** What happened to a call, as pushed to the people concerned. */
public enum CallEventType {
    /** To the members: someone is calling. */
    RINGING,
    /** To the participants: someone joined. */
    PARTICIPANT_JOINED,
    /** To the participants: someone left. */
    PARTICIPANT_LEFT,
    /** To the participants: someone has new tracks to subscribe to. */
    TRACKS_PUBLISHED,
    /** To the participants: a member refused to join. */
    DECLINED,
    /** To the members: the call is over, its status says how. */
    ENDED
}
