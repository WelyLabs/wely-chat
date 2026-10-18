package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.services.EventGroupService;
import com.calendar.chat.infrastructure.messaging.mappers.EventGroupMapper;
import com.calendar.chat.infrastructure.messaging.models.EventGroupChangedDTO;
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
import java.util.function.Function;

/**
 * Consumes {@code EVENT_GROUP_CHANGED}: keeps each event's group conversation in step with who
 * takes part.
 *
 * <p>Unlike {@link MessageBroadcastConsumer}, every pod shares one consumer group: each record must
 * be applied once, not reach every instance. A failure is retried with backoff — a dropped record
 * leaves someone outside a group they joined — then logged and dropped rather than cancelling the
 * stream, as in wely-social's consumer. Since each record carries the whole list, the next change to
 * the same event repairs what a dropped one missed.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.event-groups.enabled", havingValue = "true", matchIfMissing = true)
public class EventGroupConsumer {

    private static final int MAX_ATTEMPTS = 3;
    private static final Duration FIRST_BACKOFF = Duration.ofSeconds(1);

    private final EventGroupService eventGroupService;
    private final EventGroupMapper mapper;
    private final ObjectMapper objectMapper;

    public EventGroupConsumer(EventGroupService eventGroupService, EventGroupMapper mapper, ObjectMapper objectMapper) {
        this.eventGroupService = eventGroupService;
        this.mapper = mapper;
        this.objectMapper = objectMapper;
    }

    @Bean
    public Function<Flux<Message<byte[]>>, Mono<Void>> eventGroup() {
        return records -> records.concatMap(this::apply).then();
    }

    private Mono<Void> apply(Message<byte[]> record) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.getPayload(), EventGroupChangedDTO.class))
                .onErrorResume(error -> {
                    log.error("EVENT_GROUP_CHANGED record unreadable, dropped: {}", error.getMessage());
                    return Mono.empty();
                })
                .flatMap(this::applyGroup);
    }

    private Mono<Void> applyGroup(EventGroupChangedDTO group) {
        return Mono.defer(() -> eventGroupService.apply(mapper.toEventGroup(group)))
                .retryWhen(Retry.backoff(MAX_ATTEMPTS, FIRST_BACKOFF))
                .onErrorResume(error -> {
                    log.error("EVENT_GROUP_CHANGED dropped after {} attempts, eventId={} version={}: {}",
                            MAX_ATTEMPTS, group.eventId(), group.version(), error.getMessage(), error);
                    return Mono.empty();
                });
    }
}
