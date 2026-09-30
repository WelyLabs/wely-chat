package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

/**
 * Reads the broadcast topic and hands each record to {@link KafkaMessageBroadcaster}.
 *
 * <p>Errors are dropped per record on purpose, which is the opposite of the choice made in
 * wely-social's consumer. There, a lost {@code USER_CREATED} leaves a user with no node in
 * the graph, so it is retried with backoff. Here the message is already in MongoDB before it
 * reaches the topic: a record that fails to dispatch costs one live update, and a retry would
 * only risk delivering it twice. What must not happen is the error reaching the subscriber and
 * cancelling the subscription — that would stop this instance receiving anything at all,
 * silently, which is exactly the bug fixed in wely-social.
 *
 * <p>Which is why the per-record failure is caught inside {@code concatMap} and not with
 * {@code onErrorContinue}. That operator only rescues errors it can attribute to an element,
 * and a stream-level failure — the connection dropping, a payload the binder cannot convert —
 * goes straight past it to a {@code subscribe()} with no error handler, where Reactor rethrows
 * it as {@code ErrorCallbackNotImplemented} onto whichever thread happened to signal it.
 * A test of exactly that case is what surfaced this.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "kafka")
public class MessageBroadcastConsumer {

    private final KafkaMessageBroadcaster broadcaster;

    public MessageBroadcastConsumer(KafkaMessageBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Bean
    public Consumer<Flux<Message<MessageBroadcastDTO>>> messageBroadcast() {
        return flux -> flux
                .concatMap(this::dispatch)
                .subscribe(
                        unused -> { },
                        error -> log.error("Broadcast stream terminated; this instance will "
                                + "stop receiving live messages until it restarts", error));
    }

    private Mono<Void> dispatch(Message<MessageBroadcastDTO> record) {
        MessageBroadcastDTO payload = record.getPayload();

        return Mono.<Void>fromRunnable(() -> broadcaster.accept(payload))
                .onErrorResume(error -> {
                    log.error("Dropped one broadcast record for {}: {}",
                            payload.receiverId(), error.getMessage(), error);
                    return Mono.empty();
                });
    }
}
