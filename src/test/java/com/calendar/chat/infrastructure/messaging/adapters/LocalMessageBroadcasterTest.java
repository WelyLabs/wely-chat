package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.Duration;

class LocalMessageBroadcasterTest {

    private LocalMessageBroadcaster broadcaster;

    @BeforeEach
    void setUp() {
        broadcaster = new LocalMessageBroadcaster();
    }

    private static LiveMessage to(String receiverId) {
        return new LiveMessage(new Message("m-" + receiverId, "sender", "Sender", receiverId, "conv-1", "hi",
                Instant.now()), BroadcastKind.NEW);
    }

    @Test
    @DisplayName("a subscriber only sees the messages addressed to them")
    void subscribe_shouldFilterByRecipient() {
        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> {
                    broadcaster.broadcast(to("bob")).subscribe();
                    broadcaster.broadcast(to("alice")).subscribe();
                })
                .assertNext(live -> {
                    if (!"alice".equals(live.message().receiverId())) {
                        throw new AssertionError("delivered a message addressed to someone else");
                    }
                })
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("a null recipient does not blow up the stream")
    void subscribe_shouldTolerateANullRecipient() {
        // The previous filter called receiverId().equals(userId), which throws on a message
        // whose recipient is absent and kills every subscriber's stream with it.
        LiveMessage withoutRecipient = new LiveMessage(new Message("m-1", "sender", "Sender", null, "conv-1", "hi",
                Instant.now()), BroadcastKind.NEW);

        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> {
                    broadcaster.broadcast(withoutRecipient).subscribe();
                    broadcaster.broadcast(to("alice")).subscribe();
                })
                .assertNext(live -> {
                    if (!"alice".equals(live.message().receiverId())) {
                        throw new AssertionError("wrong message delivered");
                    }
                })
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("the sink still delivers after its last subscriber has left")
    void subscribe_shouldOutliveItsSubscribers() {
        // This is what caught the autoCancel default: the two verifiers subscribe one after
        // the other, so the sink is left with no subscriber in between — exactly what happens
        // when the last chat window in a pod closes. With autoCancel on, the sink terminated
        // there and the second subscriber got an immediate onComplete.
        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> broadcaster.broadcast(to("alice")).subscribe())
                .expectNextCount(1)
                .thenCancel()
                .verify(Duration.ofSeconds(10));

        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> broadcaster.broadcast(to("alice")).subscribe())
                .expectNextCount(1)
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void subscribe_shouldDeliverAnUpdateWithItsKind() {
        LiveMessage update = new LiveMessage(new Message("m-1", "sender", "Sender", "alice", "conv-1", "",
                Instant.now()), BroadcastKind.UPDATED);

        StepVerifier.create(broadcaster.subscribe("alice"))
                .then(() -> broadcaster.broadcast(update).subscribe())
                .expectNext(update)
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("broadcasting with nobody listening completes rather than failing")
    void broadcast_shouldCompleteWithNoSubscriber() {
        StepVerifier.create(broadcaster.broadcast(to("nobody"))).expectComplete().verify(Duration.ofSeconds(10));
    }
}
