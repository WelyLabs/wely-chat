package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.infrastructure.messaging.models.EventDateFixedDTO;
import com.calendar.chat.infrastructure.messaging.notices.DateFixedNoticeFormatter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * Consumes {@code EVENT_DATE_FIXED}: posts a notice in the event's group saying when it takes place.
 *
 * <p>One consumer group for every pod, as for {@link EventGroupConsumer}: each notice is posted once.
 * The group may not exist yet when the record arrives — wely-events announces it on another topic —
 * so a failure is retried with backoff, then logged and dropped rather than cancelling the stream.
 *
 * <p>Records arrive as bytes and are read here, as in {@link MessageBroadcastConsumer}: converted by
 * the binder, one unreadable record would end the stream for good.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.event-notices.enabled", havingValue = "true", matchIfMissing = true)
public class EventDateFixedConsumer {

    private static final int MAX_ATTEMPTS = 3;
    private static final Duration FIRST_BACKOFF = Duration.ofSeconds(1);

    private final ChatService chatService;
    private final DateFixedNoticeFormatter formatter;
    private final ObjectMapper objectMapper;

    public EventDateFixedConsumer(ChatService chatService, DateFixedNoticeFormatter formatter,
                                  ObjectMapper objectMapper) {
        this.chatService = chatService;
        this.formatter = formatter;
        this.objectMapper = objectMapper;
    }

    @Bean
    public Consumer<Flux<Message<byte[]>>> eventDateFixed() {
        return flux -> flux
                .concatMap(this::apply)
                .subscribe(
                        unused -> { },
                        error -> log.error("EVENT_DATE_FIXED subscription terminated; "
                                + "no further notices will be posted until restart", error));
    }

    private Mono<Void> apply(Message<byte[]> record) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.getPayload(), EventDateFixedDTO.class))
                .onErrorResume(error -> {
                    log.error("EVENT_DATE_FIXED record unreadable, dropped: {}", error.getMessage());
                    return Mono.empty();
                })
                .flatMap(this::post);
    }

    private Mono<Void> post(EventDateFixedDTO fixed) {
        return Mono.defer(() -> chatService.postSystemNotice(fixed.eventId(), formatter.format(fixed.start(), fixed.end())))
                .retryWhen(Retry.backoff(MAX_ATTEMPTS, FIRST_BACKOFF))
                .onErrorResume(error -> {
                    log.error("EVENT_DATE_FIXED dropped after {} attempts, eventId={}: {}",
                            MAX_ATTEMPTS, fixed.eventId(), error.getMessage(), error);
                    return Mono.empty();
                })
                .then();
    }
}
