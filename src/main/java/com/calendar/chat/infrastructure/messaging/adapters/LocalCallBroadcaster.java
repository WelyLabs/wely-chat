package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.ports.CallBroadcaster;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.util.Objects;

/**
 * Delivers call events inside a single JVM, like {@link LocalMessageBroadcaster}: correct at one
 * replica only, and chosen by the same {@code chat.broadcast.mode=local}.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "local", matchIfMissing = true)
public class LocalCallBroadcaster implements CallBroadcaster {

    /** {@code autoCancel = false}: the sink must outlive its last subscriber. */
    private final Sinks.Many<CallEvent> sink =
            Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

    @Override
    public Mono<Void> broadcast(CallEvent event) {
        return Mono.fromRunnable(() -> {
            Sinks.EmitResult result = sink.tryEmitNext(event);
            if (result.isFailure()) {
                log.error("Dropped a {} call event for {}: {}", event.type(), event.recipientId(), result);
            }
        });
    }

    @Override
    public Flux<CallEvent> subscribe(String userId) {
        return sink.asFlux().filter(event -> Objects.equals(event.recipientId(), userId));
    }
}
