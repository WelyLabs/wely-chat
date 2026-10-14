package com.calendar.chat.infrastructure.messaging.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class EventDateFixedDTOTest {

    private static final Instant START = Instant.parse("2026-10-15T18:00:00Z");
    private static final Instant END = Instant.parse("2026-10-15T20:00:00Z");

    @Test
    void isMove_shouldBeTrue_whenThePreviousStartIsGiven() {
        assertThat(new EventDateFixedDTO("e1", "Bowling", START, END, "olivia", START.minusSeconds(3600), null).isMove())
                .isTrue();
    }

    @Test
    void isMove_shouldBeFalse_forADateFixedByAPoll() {
        assertThat(new EventDateFixedDTO("e1", "Bowling", START, END, "olivia", null, null).isMove()).isFalse();
    }
}
