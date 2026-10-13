package com.calendar.chat.infrastructure.messaging.notices;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Words the notice posted in an event's group when its date is fixed, in French and one time zone. */
@Component
public class DateFixedNoticeFormatter {

    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("EEEE d MMMM", Locale.FRENCH);
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm", Locale.FRENCH);
    private static final String PREFIX = "📅 Date fixée : ";

    private final ZoneId zone;

    public DateFixedNoticeFormatter(@Value("${app.notices.zone}") ZoneId zone) {
        this.zone = zone;
    }

    /** "jeudi 16 octobre, 19:00 – 21:00" on one day, "du … au …" across several; the start alone without an end. */
    public String format(Instant start, Instant end) {
        ZonedDateTime from = start.atZone(zone);
        if (end == null || !end.isAfter(start)) {
            return PREFIX + DAY.format(from) + ", " + TIME.format(from);
        }
        ZonedDateTime to = end.atZone(zone);
        if (from.toLocalDate().equals(to.toLocalDate())) {
            return PREFIX + DAY.format(from) + ", " + TIME.format(from) + " – " + TIME.format(to);
        }
        return PREFIX + "du " + DAY.format(from) + " " + TIME.format(from)
                + " au " + DAY.format(to) + " " + TIME.format(to);
    }
}
