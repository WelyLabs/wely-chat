package com.calendar.chat.domain.models;

/**
 * A WebRTC offer or answer, as the browser produces and reads it.
 *
 * @param type "offer" or "answer"
 */
public record SessionDescription(String type, String sdp) {

    public static SessionDescription answer(String sdp) {
        return new SessionDescription("answer", sdp);
    }
}
