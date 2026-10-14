package com.calendar.chat.domain.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CallTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");

    private static CallParticipant participant(String userId) {
        return new CallParticipant(userId, "session-" + userId, NOW, NOW, List.of());
    }

    private static Call callWith(CallStatus status, List<CallParticipant> participants) {
        return new Call("call-1", "conv-1", "alice", CallMedia.VIDEO, status, NOW, null,
                List.of("alice", "bob", "carol"), participants);
    }

    @Test
    void ringing_shouldStartWithNobodyInTheCall() {
        Call call = Call.ringing("conv-1", "alice", CallMedia.AUDIO, NOW, List.of("alice", "bob"));

        assertThat(call.id()).isNull();
        assertThat(call.status()).isEqualTo(CallStatus.RINGING);
        assertThat(call.participants()).isEmpty();
        assertThat(call.memberIds()).containsExactly("alice", "bob");
        assertThat(call.endedAt()).isNull();
    }

    @Test
    void isLive_shouldFollowTheStatus() {
        assertThat(callWith(CallStatus.ACTIVE, List.of()).isLive()).isTrue();
        assertThat(callWith(CallStatus.ENDED, List.of()).isLive()).isFalse();
    }

    @Test
    void participant_shouldFindSomeoneInTheCall_andNobodyElse() {
        Call call = callWith(CallStatus.ACTIVE, List.of(participant("alice"), participant("bob")));

        assertThat(call.participant("bob")).map(CallParticipant::sessionId).contains("session-bob");
        assertThat(call.participant("carol")).isEmpty();
    }

    @Test
    void participantIds_shouldListEveryoneInTheCall() {
        Call call = callWith(CallStatus.ACTIVE, List.of(participant("alice"), participant("bob")));

        assertThat(call.participantIds()).containsExactly("alice", "bob");
        assertThat(call.participantIdsExcept("alice")).containsExactly("bob");
    }

    @Test
    void memberIdsExcept_shouldLeaveOutTheGivenMember() {
        assertThat(callWith(CallStatus.RINGING, List.of()).memberIdsExcept("alice")).containsExactly("bob", "carol");
    }
}
