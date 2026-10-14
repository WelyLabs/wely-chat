package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.infrastructure.persistence.models.entities.CallEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The call queries against a real MongoDB: the unique index on live calls, and the conditional
 * updates that keep concurrent joins and leaves consistent. None of it can be checked on a mock.
 *
 * <p>Opt-in: CI sets {@code CI=true}, locally pass {@code -Dintegration.tests=true}.
 */
@Testcontainers
@SpringBootTest(properties = "spring.data.mongodb.auto-index-creation=true")
@ActiveProfiles("test")
@EnabledIf("containersRequested")
class MongoCallRepositoryAdapterIntegrationTest {

    @Container
    @ServiceConnection
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static boolean containersRequested() {
        return System.getenv("CI") != null || Boolean.getBoolean("integration.tests");
    }

    private static final Instant T0 = Instant.parse("2026-10-15T18:00:00Z");

    @Autowired
    private MongoCallRepositoryAdapter adapter;

    @Autowired
    private ReactiveMongoTemplate mongoTemplate;

    @BeforeEach
    void clear() {
        // Documents, not the collection: dropping takes the unique index with it.
        mongoTemplate.remove(new Query(), CallEntity.class).block();
    }

    private Call start(String conversationId) {
        return adapter.createOrGetLive(Call.ringing(conversationId, "alice", CallMedia.VIDEO, T0,
                List.of("alice", "bob", "carol"))).block();
    }

    private static CallParticipant participant(String userId, Instant seenAt) {
        return new CallParticipant(userId, "s-" + userId, T0, seenAt, List.of());
    }

    @Test
    @DisplayName("two calls started at once in a conversation end up as one")
    void createOrGetLive_shouldKeepOneLiveCallPerConversation() {
        List<Call> started = Flux.range(0, 5)
                .flatMap(attempt -> adapter.createOrGetLive(Call.ringing("conv-1", "user-" + attempt,
                        CallMedia.AUDIO, T0, List.of("alice"))))
                .collectList().block();

        assertThat(started).extracting(Call::id).containsOnly(started.getFirst().id());
    }

    @Test
    @DisplayName("an ended call no longer blocks a new one")
    void createOrGetLive_shouldStartANewCall_afterTheLastOneEnded() {
        Call first = start("conv-1");
        adapter.end(first.id(), CallStatus.ENDED, T0).block();

        Call second = start("conv-1");

        assertThat(second.id()).isNotEqualTo(first.id());
        StepVerifier.create(adapter.findLive("conv-1").map(Call::id)).expectNext(second.id()).verifyComplete();
    }

    @Test
    @DisplayName("a call is only found by its members")
    void findById_shouldNotFindSomeoneElsesCall() {
        Call call = start("conv-1");

        StepVerifier.create(adapter.findById(call.id(), "bob").map(Call::id)).expectNext(call.id()).verifyComplete();
        StepVerifier.create(adapter.findById(call.id(), "mallory")).verifyComplete();
    }

    @Test
    @DisplayName("the last place goes to one of two people joining at once")
    void addParticipant_shouldNeverGoPastTheCap() {
        Call call = start("conv-1");
        adapter.addParticipant(call.id(), participant("alice", T0), 2).block();

        List<Call> joined = Flux.just("bob", "carol")
                .flatMap(userId -> adapter.addParticipant(call.id(), participant(userId, T0), 2))
                .collectList().block();

        assertThat(joined).hasSize(1);
        assertThat(adapter.findById(call.id(), "alice").block().participants()).hasSize(2);
    }

    @Test
    @DisplayName("joining again replaces the previous session instead of adding a second entry")
    void addParticipant_shouldReplaceTheParticipantsPreviousEntry() {
        Call call = start("conv-1");
        adapter.addParticipant(call.id(), participant("bob", T0), 12).block();

        Call rejoined = adapter.addParticipant(call.id(),
                new CallParticipant("bob", "s-bob-2", T0, T0, List.of()), 12).block();

        assertThat(rejoined.participants()).singleElement()
                .satisfies(bob -> assertThat(bob.sessionId()).isEqualTo("s-bob-2"));
    }

    @Test
    @DisplayName("tracks, heartbeats and leaving touch only the participant concerned")
    void participantWrites_shouldTargetOneParticipant() {
        Call call = start("conv-1");
        adapter.addParticipant(call.id(), participant("alice", T0), 12).block();
        adapter.addParticipant(call.id(), participant("bob", T0), 12).block();

        adapter.addTracks(call.id(), "bob", List.of("audio", "camera")).block();
        adapter.addTracks(call.id(), "bob", List.of("audio")).block();
        adapter.touch(call.id(), "bob", T0.plusSeconds(15)).block();
        Call afterLeave = adapter.removeParticipant(call.id(), "alice").block();

        assertThat(afterLeave.participants()).singleElement().satisfies(bob -> {
            assertThat(bob.trackNames()).containsExactly("audio", "camera");
            assertThat(bob.lastSeenAt()).isEqualTo(T0.plusSeconds(15));
        });
        StepVerifier.create(adapter.touch(call.id(), "alice", T0)).verifyComplete();
    }

    @Test
    @DisplayName("a call ends once, and nobody can join it afterwards")
    void end_shouldEndOnce() {
        Call call = start("conv-1");

        StepVerifier.create(adapter.end(call.id(), CallStatus.MISSED, T0.plusSeconds(30)))
                .assertNext(ended -> {
                    assertThat(ended.status()).isEqualTo(CallStatus.MISSED);
                    assertThat(ended.endedAt()).isEqualTo(T0.plusSeconds(30));
                })
                .verifyComplete();
        StepVerifier.create(adapter.end(call.id(), CallStatus.ENDED, T0)).verifyComplete();
        StepVerifier.create(adapter.addParticipant(call.id(), participant("bob", T0), 12)).verifyComplete();
    }

    @Test
    @DisplayName("the reaper's queries find what is overdue, and nothing ended")
    void finders_shouldFindOverdueLiveCalls() {
        Call ringing = start("conv-1");
        Call active = start("conv-2");
        adapter.addParticipant(active.id(), participant("alice", T0), 12).block();
        adapter.markActive(active.id()).block();
        Call ended = start("conv-3");
        adapter.end(ended.id(), CallStatus.ENDED, T0).block();

        StepVerifier.create(adapter.findRingingStartedBefore(T0.plusSeconds(1)).map(Call::id).collectList())
                .assertNext(ids -> assertThat(ids).containsExactly(ringing.id()))
                .verifyComplete();
        StepVerifier.create(adapter.findLiveWithParticipantSeenBefore(T0.plusSeconds(1)).map(Call::id))
                .expectNext(active.id()).verifyComplete();
        StepVerifier.create(adapter.findLiveStartedBefore(T0.plusSeconds(1)).map(Call::id).collectList())
                .assertNext(ids -> assertThat(ids).containsExactlyInAnyOrder(ringing.id(), active.id()))
                .verifyComplete();
    }
}
