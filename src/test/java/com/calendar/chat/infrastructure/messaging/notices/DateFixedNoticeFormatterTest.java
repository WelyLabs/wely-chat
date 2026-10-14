package com.calendar.chat.infrastructure.messaging.notices;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class DateFixedNoticeFormatterTest {

    private static final ZoneId PARIS = ZoneId.of("Europe/Paris");

    private final DateFixedNoticeFormatter paris = new DateFixedNoticeFormatter(PARIS);

    @Test
    @DisplayName("an event within one day reads as its day, then its hours")
    void format_shouldWriteOneDay_whenStartAndEndFallOnTheSameDay() {
        String notice = paris.format(Instant.parse("2026-10-15T17:00:00Z"), Instant.parse("2026-10-15T19:00:00Z"));

        assertThat(notice).isEqualTo("📅 Date fixée : jeudi 15 octobre, 19:00 – 21:00");
    }

    @Test
    @DisplayName("an event over midnight reads as from one day to the next")
    void format_shouldWriteBothDays_whenTheEventSpansSeveralDays() {
        String notice = paris.format(Instant.parse("2026-10-15T20:00:00Z"), Instant.parse("2026-10-16T00:00:00Z"));

        assertThat(notice).isEqualTo("📅 Date fixée : du jeudi 15 octobre 22:00 au vendredi 16 octobre 02:00");
    }

    @Test
    @DisplayName("days and hours are those of the configured zone, not UTC")
    void format_shouldUseTheConfiguredZone() {
        Instant start = Instant.parse("2026-10-15T22:30:00Z");
        Instant end = Instant.parse("2026-10-15T23:30:00Z");

        assertThat(new DateFixedNoticeFormatter(ZoneOffset.UTC).format(start, end))
                .isEqualTo("📅 Date fixée : jeudi 15 octobre, 22:30 – 23:30");
        assertThat(paris.format(start, end))
                .isEqualTo("📅 Date fixée : vendredi 16 octobre, 00:30 – 01:30");
    }

    @Test
    void format_shouldFollowTheWinterOffset() {
        assertThat(paris.format(Instant.parse("2026-12-03T18:00:00Z"), Instant.parse("2026-12-03T20:00:00Z")))
                .isEqualTo("📅 Date fixée : jeudi 3 décembre, 19:00 – 21:00");
    }

    @Test
    void format_shouldWriteTheStartAlone_whenThereIsNoEnd() {
        assertThat(paris.format(Instant.parse("2026-10-15T17:00:00Z"), null))
                .isEqualTo("📅 Date fixée : jeudi 15 octobre, 19:00");
    }

    @Test
    void format_shouldWriteTheStartAlone_whenTheEndIsNotAfterIt() {
        Instant start = Instant.parse("2026-10-15T17:00:00Z");

        assertThat(paris.format(start, start)).isEqualTo("📅 Date fixée : jeudi 15 octobre, 19:00");
        assertThat(paris.format(start, start.minusSeconds(60))).isEqualTo("📅 Date fixée : jeudi 15 octobre, 19:00");
    }

    @Test
    @DisplayName("a move within the day gives the new hours, then the previous ones")
    void formatMove_shouldGiveThePreviousHoursAlone_whenTheDayStaysTheSame() {
        String notice = paris.formatMove(Instant.parse("2026-10-15T18:00:00Z"), Instant.parse("2026-10-15T20:00:00Z"),
                Instant.parse("2026-10-15T17:00:00Z"), Instant.parse("2026-10-15T19:00:00Z"));

        assertThat(notice).isEqualTo("🔁 Sortie décalée : jeudi 15 octobre, 20:00 – 22:00 (au lieu de 19:00 – 21:00)");
    }

    @Test
    @DisplayName("a move to another day gives the previous day too")
    void formatMove_shouldGiveThePreviousDate_whenTheDayChanges() {
        String notice = paris.formatMove(Instant.parse("2026-10-16T17:00:00Z"), Instant.parse("2026-10-16T19:00:00Z"),
                Instant.parse("2026-10-15T17:00:00Z"), Instant.parse("2026-10-15T19:00:00Z"));

        assertThat(notice).isEqualTo("🔁 Sortie décalée : vendredi 16 octobre, 19:00 – 21:00 (au lieu de jeudi 15 octobre, 19:00 – 21:00)");
    }

    @Test
    void formatMove_shouldGiveThePreviousStartAlone_whenItHadNoEnd() {
        String notice = paris.formatMove(Instant.parse("2026-10-15T18:00:00Z"), Instant.parse("2026-10-15T19:00:00Z"),
                Instant.parse("2026-10-15T17:00:00Z"), null);

        assertThat(notice).isEqualTo("🔁 Sortie décalée : jeudi 15 octobre, 20:00 – 21:00 (au lieu de 19:00)");
    }

    @Test
    void formatMove_shouldWriteBothDaysOfEither_whenOneSpansSeveralDays() {
        String notice = paris.formatMove(Instant.parse("2026-10-15T20:00:00Z"), Instant.parse("2026-10-16T00:00:00Z"),
                Instant.parse("2026-10-15T17:00:00Z"), Instant.parse("2026-10-15T19:00:00Z"));

        assertThat(notice).isEqualTo("🔁 Sortie décalée : du jeudi 15 octobre 22:00 au vendredi 16 octobre 02:00"
                + " (au lieu de jeudi 15 octobre, 19:00 – 21:00)");
    }

    @Test
    void formatMove_shouldGiveTheWholePreviousDate_whenThePreviousOneSpannedSeveralDays() {
        String notice = paris.formatMove(Instant.parse("2026-10-15T17:00:00Z"), Instant.parse("2026-10-15T19:00:00Z"),
                Instant.parse("2026-10-15T08:00:00Z"), Instant.parse("2026-10-16T08:00:00Z"));

        assertThat(notice).isEqualTo("🔁 Sortie décalée : jeudi 15 octobre, 19:00 – 21:00"
                + " (au lieu du jeudi 15 octobre 10:00 au vendredi 16 octobre 10:00)");
    }
}
