package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallEventType;
import com.calendar.chat.domain.models.CallMedia;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

class LocalCallBroadcasterTest {

    private final LocalCallBroadcaster broadcaster = new LocalCallBroadcaster();

    private static CallEvent ringing(String recipientId) {
        return new CallEvent(CallEventType.RINGING, recipientId, "alice",
                Call.ringing("conv-1", "alice", CallMedia.AUDIO, Instant.EPOCH, List.of("alice", recipientId)));
    }

    @Test
    void subscribe_shouldOnlyReceiveTheEventsAddressedToTheUser() {
        StepVerifier.create(broadcaster.subscribe("bob").take(1))
                .then(() -> {
                    broadcaster.broadcast(ringing("carol")).block();
                    broadcaster.broadcast(ringing("bob")).block();
                })
                .expectNext(ringing("bob"))
                .verifyComplete();
    }

    @Test
    void broadcast_shouldKeepDelivering_afterEverySubscriberLeft() {
        StepVerifier.create(broadcaster.subscribe("bob").take(1))
                .then(() -> broadcaster.broadcast(ringing("bob")).block())
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(broadcaster.subscribe("bob").take(1))
                .then(() -> broadcaster.broadcast(ringing("bob")).block())
                .expectNextCount(1)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }
}
