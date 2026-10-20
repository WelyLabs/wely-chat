package com.calendar.chat.domain.ports;

import java.time.Duration;

/**
 * Business facts worth counting, reported the moment they are established — after the write
 * succeeded, never on a failure. How they are counted is the adapter's business: the domain only
 * says what happened, and never knows about Micrometer.
 */
public interface ChatActivity {

    /** What a sent message carries. */
    enum MessageKind { TEXT, PHOTO }

    /** A member's message was stored in its conversation. Notices the application posts are not messages. */
    void messageSent(MessageKind kind);

    /** A call started ringing; joining a call that is already live starts nothing. */
    void callStarted();

    /** An answered call ended, after {@code duration} from its first ring. Missed and declined calls never ran. */
    void callEnded(Duration duration);

    /** A live message stream ({@code chat.stream}) opened. */
    void streamOpened();

    /** A live message stream closed: completed, failed or cancelled by the client. */
    void streamClosed();
}
