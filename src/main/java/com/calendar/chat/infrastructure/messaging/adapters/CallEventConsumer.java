package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.exception.GlobalErrorHandler;
import com.calendar.chat.exception.TechnicalErrorCounter;
import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
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
 * Reads the call events topic and hands each record to {@link KafkaCallBroadcaster}. Same choices
 * as {@link MessageBroadcastConsumer}: records arrive as bytes and are read one at a time, and a
 * record that fails is dropped inside {@code concatMap}, so it never ends the stream. A function
 * returning {@code Mono<Void>}: the binder subscribes.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "kafka")
public class CallEventConsumer {

    private final KafkaCallBroadcaster broadcaster;
    private final ObjectMapper objectMapper;
    private final TechnicalErrorCounter errors;

    public CallEventConsumer(KafkaCallBroadcaster broadcaster,
                             ObjectMapper objectMapper,
                             TechnicalErrorCounter errors) {
        this.broadcaster = broadcaster;
        this.objectMapper = objectMapper;
        this.errors = errors;
    }

    @Bean
    public Function<Flux<Message<byte[]>>, Mono<Void>> callEvents() {
        return records -> records.concatMap(this::dispatch).then();
    }

    private Mono<Void> dispatch(Message<byte[]> record) {
        return Mono.<Void>fromRunnable(() -> broadcaster.accept(
                        objectMapper.readValue(record.getPayload(), CallEventBroadcastDTO.class)))
                .onErrorResume(error -> {
                    errors.count(error, GlobalErrorHandler.SOURCE_KAFKA);
                    log.error("Dropped one call event record: {}", error.getMessage());
                    return Mono.empty();
                });
    }
}
