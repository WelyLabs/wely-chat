package com.calendar.chat.infrastructure.messaging.notices;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CallSummaryFormatterTest {

    private static final Instant START = Instant.parse("2026-10-15T18:00:00Z");

    private final CallSummaryFormatter formatter = new CallSummaryFormatter();

    private static Call call(CallMedia media, CallStatus status, Duration length) {
        return new Call("call-1", "conv-1", "alice", media, status, START, START.plus(length), List.of("alice", "bob"),
                List.of());
    }

    @Test
    void format_shouldGiveAnAnsweredCallsKindAndLength() {
        assertThat(formatter.format(call(CallMedia.VIDEO, CallStatus.ENDED, Duration.ofMinutes(12))))
                .isEqualTo("📞 Appel vidéo · 12 min");
    }

    @Test
    void format_shouldSayAnUnansweredCallWasMissed() {
        assertThat(formatter.format(call(CallMedia.AUDIO, CallStatus.MISSED, Duration.ofSeconds(30))))
                .isEqualTo("📞 Appel audio manqué");
    }

    @Test
    void format_shouldSayARefusedCallWasRefused() {
        assertThat(formatter.format(call(CallMedia.VIDEO, CallStatus.DECLINED, Duration.ofSeconds(5))))
                .isEqualTo("📞 Appel vidéo refusé");
    }

    @ParameterizedTest
    @CsvSource({"45,45 s", "60,1 min", "3599,59 min", "3600,1 h 00", "3900,1 h 05"})
    void duration_shouldReadNaturally(long seconds, String expected) {
        assertThat(CallSummaryFormatter.duration(Duration.ofSeconds(seconds))).isEqualTo(expected);
    }
}
