package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.util.function.Consumer;

/**
 * Reads the call events topic and hands each record to {@link KafkaCallBroadcaster}. Same choices
 * as {@link MessageBroadcastConsumer}: records arrive as bytes and are read one at a time, and a
 * record that fails is dropped inside {@code concatMap}, so it never ends the stream.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "kafka")
public class CallEventConsumer {

    private final KafkaCallBroadcaster broadcaster;
    private final ObjectMapper objectMapper;

    public CallEventConsumer(KafkaCallBroadcaster broadcaster, ObjectMapper objectMapper) {
        this.broadcaster = broadcaster;
        this.objectMapper = objectMapper;
    }

    @Bean
    public Consumer<Flux<Message<byte[]>>> callEvents() {
        return flux -> flux
                .concatMap(this::dispatch)
                .subscribe(
                        unused -> { },
                        error -> log.error("Call events stream terminated; this instance will "
                                + "stop receiving call events until it restarts", error));
    }

    private Mono<Void> dispatch(Message<byte[]> record) {
        return Mono.<Void>fromRunnable(() -> broadcaster.accept(
                        objectMapper.readValue(record.getPayload(), CallEventBroadcastDTO.class)))
                .onErrorResume(error -> {
                    log.error("Dropped one call event record: {}", error.getMessage());
                    return Mono.empty();
                });
    }
}
