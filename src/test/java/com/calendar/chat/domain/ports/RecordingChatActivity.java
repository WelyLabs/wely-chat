package com.calendar.chat.domain.ports;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Test double of {@link ChatActivity}: records each fact, so a domain test checks them without Micrometer. */
public class RecordingChatActivity implements ChatActivity {

    public final List<MessageKind> messagesSent = new ArrayList<>();
    public final List<Duration> callsEnded = new ArrayList<>();
    public int callsStarted;
    public int streamsOpened;
    public int streamsClosed;

    @Override
    public void messageSent(MessageKind kind) {
        messagesSent.add(kind);
    }

    @Override
    public void callStarted() {
        callsStarted++;
    }

    @Override
    public void callEnded(Duration duration) {
        callsEnded.add(duration);
    }

    @Override
    public void streamOpened() {
        streamsOpened++;
    }

    @Override
    public void streamClosed() {
        streamsClosed++;
    }
}
