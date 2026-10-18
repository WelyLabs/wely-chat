package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.util.function.Function;

/**
 * Reads the broadcast topic and hands each record to {@link KafkaMessageBroadcaster}.
 *
 * <p>Errors are dropped per record on purpose, which is the opposite of the choice made in
 * wely-social's consumer. There, a lost {@code USER_CREATED} leaves a user with no node in
 * the graph, so it is retried with backoff. Here the message is already in MongoDB before it
 * reaches the topic: a record that fails to dispatch costs one live update, and a retry would
 * only risk delivering it twice. What must not happen is a record's error reaching the binder and
 * cancelling the subscription — that would stop this instance receiving anything at all,
 * silently, which is exactly the bug fixed in wely-social.
 *
 * <p>Which is why the per-record failure is caught inside {@code concatMap} and not with
 * {@code onErrorContinue}, which only rescues errors it can attribute to an element. A
 * stream-level failure — the connection dropping — is not caught here: the bean is a function
 * returning {@code Mono<Void>}, so the binder subscribes and that failure reaches it, instead of
 * a {@code subscribe()} of our own that could only log it.
 *
 * <p>Which is also why records arrive as bytes and are read here, one at a time. Converted by the
 * binder, a record it could not read failed the whole stream: when message times became instants,
 * every pod met a record in the old zone-less format and went deaf until restarted. Read inside
 * {@code dispatch}, an unreadable record is one more dropped record.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "kafka")
public class MessageBroadcastConsumer {

    private final KafkaMessageBroadcaster broadcaster;
    private final ObjectMapper objectMapper;

    public MessageBroadcastConsumer(KafkaMessageBroadcaster broadcaster, ObjectMapper objectMapper) {
        this.broadcaster = broadcaster;
        this.objectMapper = objectMapper;
    }

    @Bean
    public Function<Flux<Message<byte[]>>, Mono<Void>> messageBroadcast() {
        return records -> records.concatMap(this::dispatch).then();
    }

    private Mono<Void> dispatch(Message<byte[]> record) {
        return Mono.<Void>fromRunnable(() -> broadcaster.accept(
                        objectMapper.readValue(record.getPayload(), MessageBroadcastDTO.class)))
                .onErrorResume(error -> {
                    // The payload is not logged: it is someone's private message.
                    log.error("Dropped one broadcast record: {}", error.getMessage());
                    return Mono.empty();
                });
    }
}
