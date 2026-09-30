package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.util.Objects;

/**
 * Delivers messages inside a single JVM.
 *
 * <p>Correct only at one replica, and selected explicitly by
 * {@code chat.broadcast.mode=local} rather than by accident — that was the whole problem
 * with the sink this replaces: it looked fine because the deployment happened to run one
 * pod, and nothing said the design depended on that.
 *
 * <p>{@code onBackpressureBuffer} rather than {@code directBestEffort()}: the previous sink
 * dropped a message when a subscriber was momentarily not requesting, and the return value
 * of {@code tryEmitNext} was never checked, so the drop was silent. This variant buffers
 * per subscriber instead, and a slow consumer is dropped rather than everyone's message.
 *
 * <p>The {@code autoCancel = false} argument is load-bearing. It defaults to {@code true},
 * which terminates the sink for good once the last subscriber goes away — so the first time
 * every chat window in the process closed, the sink would complete and every subscriber
 * after that would get an immediate {@code onComplete} and never see another message until
 * the pod restarted.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "local", matchIfMissing = true)
public class LocalMessageBroadcaster implements MessageBroadcaster {

    private final Sinks.Many<Message> sink =
            Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

    public LocalMessageBroadcaster() {
        log.warn("Message delivery is in-process: correct at one replica only. "
                + "Set chat.broadcast.mode=kafka to fan out across instances.");
    }

    @Override
    public Mono<Void> broadcast(Message message) {
        return Mono.fromRunnable(() -> {
            Sinks.EmitResult result = sink.tryEmitNext(message);
            if (result.isFailure()) {
                // Checked, unlike before: a dropped message used to leave no trace at all.
                log.error("Dropped a message for {}: {}", message.receiverId(), result);
            }
        });
    }

    @Override
    public Flux<Message> subscribe(String userId) {
        return sink.asFlux().filter(message -> Objects.equals(message.receiverId(), userId));
    }
}
