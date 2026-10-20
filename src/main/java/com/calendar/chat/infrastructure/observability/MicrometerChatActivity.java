package com.calendar.chat.infrastructure.observability;

import com.calendar.chat.domain.ports.ChatActivity;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counts the chat's business facts for Grafana. The names and tags are a contract with the
 * dashboards; no tag ever carries a user, conversation or call id.
 *
 * <ul>
 *   <li>{@value #MESSAGES_SENT} — counter, tag {@code kind} = {@code text} | {@code photo};</li>
 *   <li>{@value #CALLS_STARTED} — counter;</li>
 *   <li>{@value #CALLS_DURATION} — timer of answered calls, fixed SLO buckets rather than a
 *       percentile histogram: this runs on a Raspberry Pi;</li>
 *   <li>{@value #STREAMS_ACTIVE} — gauge of the {@code chat.stream} subscriptions open on this instance.</li>
 * </ul>
 */
@Component
public class MicrometerChatActivity implements ChatActivity {

    static final String MESSAGES_SENT = "wely.chat.messages.sent";
    static final String CALLS_STARTED = "wely.chat.calls.started";
    static final String CALLS_DURATION = "wely.chat.calls.duration";
    static final String STREAMS_ACTIVE = "wely.chat.streams.active";
    static final String KIND_TAG = "kind";

    private final Map<MessageKind, Counter> messagesSent = new EnumMap<>(MessageKind.class);
    private final Counter callsStarted;
    private final Timer callsDuration;
    private final AtomicInteger streamsActive = new AtomicInteger();

    public MicrometerChatActivity(MeterRegistry registry) {
        for (MessageKind kind : MessageKind.values()) {
            messagesSent.put(kind, Counter.builder(MESSAGES_SENT)
                    .description("Messages stored, by what they carry")
                    .tag(KIND_TAG, kind.name().toLowerCase(Locale.ROOT))
                    .register(registry));
        }
        callsStarted = Counter.builder(CALLS_STARTED)
                .description("Calls that started ringing")
                .register(registry);
        callsDuration = Timer.builder(CALLS_DURATION)
                .description("Duration of answered calls, from the first ring to the end")
                .publishPercentileHistogram(false)
                .serviceLevelObjectives(Duration.ofSeconds(30), Duration.ofMinutes(1), Duration.ofMinutes(5),
                        Duration.ofMinutes(15), Duration.ofMinutes(30), Duration.ofHours(1))
                .register(registry);
        Gauge.builder(STREAMS_ACTIVE, streamsActive, AtomicInteger::get)
                .description("chat.stream subscriptions open on this instance")
                .register(registry);
    }

    @Override
    public void messageSent(MessageKind kind) {
        messagesSent.get(kind).increment();
    }

    @Override
    public void callStarted() {
        callsStarted.increment();
    }

    @Override
    public void callEnded(Duration duration) {
        callsDuration.record(duration);
    }

    @Override
    public void streamOpened() {
        streamsActive.incrementAndGet();
    }

    @Override
    public void streamClosed() {
        streamsActive.decrementAndGet();
    }
}
