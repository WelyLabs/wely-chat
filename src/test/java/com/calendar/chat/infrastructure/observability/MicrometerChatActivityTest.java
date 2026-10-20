package com.calendar.chat.infrastructure.observability;

import com.calendar.chat.domain.ports.ChatActivity;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerChatActivityTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerChatActivity activity = new MicrometerChatActivity(registry);

    @Test
    void messageSent_shouldCountByKind() {
        activity.messageSent(ChatActivity.MessageKind.TEXT);
        activity.messageSent(ChatActivity.MessageKind.TEXT);
        activity.messageSent(ChatActivity.MessageKind.PHOTO);

        assertThat(registry.get(MicrometerChatActivity.MESSAGES_SENT).tag("kind", "text").counter().count()).isEqualTo(2);
        assertThat(registry.get(MicrometerChatActivity.MESSAGES_SENT).tag("kind", "photo").counter().count()).isEqualTo(1);
    }

    @Test
    @DisplayName("both kinds exist from the start, at zero, so a dashboard never shows a missing series")
    void constructor_shouldRegisterEveryKind_atZero() {
        assertThat(registry.get(MicrometerChatActivity.MESSAGES_SENT).counters()).hasSize(2)
                .allSatisfy(counter -> assertThat(counter.count()).isZero());
    }

    @Test
    void callStarted_shouldCount() {
        activity.callStarted();

        assertThat(registry.get(MicrometerChatActivity.CALLS_STARTED).counter().count()).isEqualTo(1);
    }

    @Test
    @DisplayName("call durations land in the fixed SLO buckets, without a percentile histogram")
    void callEnded_shouldRecordTheDuration_inTheSloBuckets() {
        activity.callEnded(Duration.ofMinutes(3));

        Timer timer = registry.get(MicrometerChatActivity.CALLS_DURATION).timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.MINUTES)).isEqualTo(3);
        assertThat(Arrays.stream(timer.takeSnapshot().histogramCounts()).map(CountAtBucket::bucket).toList())
                .containsExactly(30e9, 60e9, 300e9, 900e9, 1800e9, 3600e9);
    }

    @Test
    void streamOpenedAndClosed_shouldMoveTheGauge() {
        activity.streamOpened();
        activity.streamOpened();
        activity.streamClosed();

        assertThat(registry.get(MicrometerChatActivity.STREAMS_ACTIVE).gauge().value()).isEqualTo(1);
    }
}
