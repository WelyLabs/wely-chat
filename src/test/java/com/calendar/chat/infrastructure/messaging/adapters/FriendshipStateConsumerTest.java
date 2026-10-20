package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.domain.services.FriendshipService;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import com.calendar.chat.infrastructure.messaging.models.FriendshipStateDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FriendshipStateConsumerTest {

    private static final String ALICE = "11111111-0000-4000-8000-000000000001";
    private static final String BOB = "22222222-0000-4000-8000-000000000002";
    private static final String KEY = ALICE + ":" + BOB;

    @Mock
    private FriendshipService friendshipService;

    private FriendshipStateConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new FriendshipStateConsumer(friendshipService, JsonMapper.builder().build());
    }

    private static Message<byte[]> value(String key, String json) {
        return MessageBuilder.withPayload(json.getBytes(StandardCharsets.UTF_8))
                .setHeader(KafkaHeaders.RECEIVED_KEY, key.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    private static Message<byte[]> friends(long version) {
        return value(KEY, "{\"a\":\"%s\",\"b\":\"%s\",\"friends\":true,\"closeness\":\"FRIEND\",\"since\":\"2026-10-01T10:00:00Z\",\"version\":%d}"
                .formatted(ALICE, BOB, version));
    }

    // The binder hands a tombstone over with a KafkaNull payload, whatever the declared type.
    @SuppressWarnings("unchecked")
    private static Message<byte[]> tombstone(String key) {
        Message<?> record = MessageBuilder.withPayload(KafkaNull.INSTANCE)
                .setHeader(KafkaHeaders.RECEIVED_KEY, key.getBytes(StandardCharsets.UTF_8))
                .build();
        return (Message<byte[]>) record;
    }

    @Test
    void friendshipState_shouldStoreThePair_fromAValue() {
        when(friendshipService.applyState(new Friendship(ALICE, BOB, 7))).thenReturn(Mono.just(true));

        StepVerifier.create(consumer.friendshipState().apply(Flux.just(friends(7)))).expectComplete().verify(Duration.ofSeconds(10));

        verify(friendshipService).applyState(new Friendship(ALICE, BOB, 7));
    }

    @Test
    void friendshipState_shouldEndThePair_fromATombstone() {
        when(friendshipService.applyTombstone(ALICE, BOB)).thenReturn(Mono.just(true));

        StepVerifier.create(consumer.friendshipState().apply(Flux.just(tombstone(KEY)))).expectComplete().verify(Duration.ofSeconds(10));

        verify(friendshipService).applyTombstone(ALICE, BOB);
    }

    @Test
    void friendshipState_shouldDropATombstoneWithAMalformedKey() {
        StepVerifier.create(consumer.friendshipState().apply(Flux.just(tombstone("not-a-pair"), tombstone(ALICE + ":" + ALICE))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(friendshipService);
    }

    @Test
    @DisplayName("a value naming another pair than its key is dropped")
    void friendshipState_shouldDropAValueThatDoesNotMatchItsKey() {
        String other = "33333333-0000-4000-8000-000000000003";

        StepVerifier.create(consumer.friendshipState().apply(Flux.just(value(ALICE + ":" + other,
                        "{\"a\":\"%s\",\"b\":\"%s\",\"friends\":true,\"version\":1}".formatted(ALICE, BOB)))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(friendshipService);
    }

    @Test
    @DisplayName("an unreadable record is dropped: the next one is still applied")
    void friendshipState_shouldDropAnUnreadableRecord_andApplyTheNext() {
        when(friendshipService.applyState(any())).thenReturn(Mono.just(true));

        StepVerifier.create(consumer.friendshipState().apply(Flux.just(value(KEY, "not json"), friends(2))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(friendshipService).applyState(new Friendship(ALICE, BOB, 2));
    }

    @Test
    void friendshipState_shouldRetryAFailedWrite_thenSucceed() {
        AtomicInteger attempts = new AtomicInteger();
        when(friendshipService.applyState(any())).thenReturn(Mono.defer(() -> attempts.incrementAndGet() < 3
                ? Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE))
                : Mono.just(true)));

        StepVerifier.withVirtualTime(() -> consumer.friendshipState().apply(Flux.just(friends(2))))
                .thenAwait(Duration.ofMinutes(5))
                .verifyComplete();

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("a write that keeps failing is skipped, not fatal: the stream goes on")
    void friendshipState_shouldSkipTheRecord_whenRetriesAreExhausted() {
        when(friendshipService.applyState(any()))
                .thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));
        when(friendshipService.applyTombstone(anyString(), anyString())).thenReturn(Mono.just(true));

        StepVerifier.withVirtualTime(() -> consumer.friendshipState().apply(Flux.just(friends(2), tombstone(KEY))))
                .thenAwait(Duration.ofMinutes(10))
                .verifyComplete();

        verify(friendshipService).applyTombstone(ALICE, BOB);
    }

    @Test
    void state_shouldReadAValue() {
        assertThat(FriendshipStateConsumer.state(new FriendshipStateDTO(BOB, ALICE, true, 4L)))
                .contains(new Friendship(ALICE, BOB, 4));
    }

    @Test
    void state_shouldRejectAMalformedValue() {
        assertThat(FriendshipStateConsumer.state(new FriendshipStateDTO(ALICE, BOB, false, 4L))).isEmpty();
        assertThat(FriendshipStateConsumer.state(new FriendshipStateDTO(ALICE, BOB, true, null))).isEmpty();
        assertThat(FriendshipStateConsumer.state(new FriendshipStateDTO(ALICE, "camille", true, 4L))).isEmpty();
        assertThat(FriendshipStateConsumer.state(new FriendshipStateDTO(ALICE, ALICE, true, 4L))).isEmpty();
    }

    @Test
    void pairOfKey_shouldReadBytesOrAString() {
        assertThat(FriendshipStateConsumer.pairOfKey(KEY.getBytes(StandardCharsets.UTF_8))).hasValueSatisfying(
                pair -> assertThat(pair).containsExactly(ALICE, BOB));
        assertThat(FriendshipStateConsumer.pairOfKey(KEY)).hasValueSatisfying(
                pair -> assertThat(pair).containsExactly(ALICE, BOB));
    }

    @Test
    void pairOfKey_shouldRejectAMalformedKey() {
        assertThat(FriendshipStateConsumer.pairOfKey(null)).isEmpty();
        assertThat(FriendshipStateConsumer.pairOfKey(ALICE)).isEmpty();
        assertThat(FriendshipStateConsumer.pairOfKey(ALICE + ":" + BOB + ":" + ALICE)).isEmpty();
        assertThat(FriendshipStateConsumer.pairOfKey(ALICE + ":" + ALICE)).isEmpty();
    }

    @Test
    void canonical_shouldAcceptOnlyAUuid_inLowerCase() {
        assertThat(FriendshipStateConsumer.canonical(ALICE)).contains(ALICE);
        assertThat(FriendshipStateConsumer.canonical(ALICE.toUpperCase())).contains(ALICE);
        assertThat(FriendshipStateConsumer.canonical("camille")).isEmpty();
        assertThat(FriendshipStateConsumer.canonical(null)).isEmpty();
    }

    @Test
    void friendshipState_shouldNeverApplyAValueThatIsNotFriends() {
        StepVerifier.create(consumer.friendshipState().apply(Flux.just(value(KEY,
                        "{\"a\":\"%s\",\"b\":\"%s\",\"friends\":false,\"version\":1}".formatted(ALICE, BOB)))))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(friendshipService, never()).applyState(any());
    }
}
