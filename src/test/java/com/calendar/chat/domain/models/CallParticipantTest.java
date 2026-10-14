package com.calendar.chat.domain.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CallParticipantTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");

    @Test
    void publishes_shouldTellWhichTracksTheParticipantSends() {
        CallParticipant participant = new CallParticipant("alice", "s-1", NOW, NOW, List.of("audio"));

        assertThat(participant.publishes("audio")).isTrue();
        assertThat(participant.publishes("camera")).isFalse();
    }

    @Test
    void constructor_shouldCopyTheTracks_soTheCallerCannotChangeThemAfterwards() {
        List<String> tracks = new ArrayList<>(List.of("audio"));
        CallParticipant participant = new CallParticipant("alice", "s-1", NOW, NOW, tracks);

        tracks.add("camera");

        assertThat(participant.trackNames()).containsExactly("audio");
    }
}
