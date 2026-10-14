package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.ports.CallBroadcaster;
import com.calendar.chat.infrastructure.messaging.mappers.CallEventBroadcastMapper;
import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.util.Objects;

/**
 * Fans call events out across every instance, the same way {@link KafkaMessageBroadcaster} does
 * for messages: each pod is its own consumer group, so each receives every record and hands it
 * to the streams it holds. Best-effort too: the call's state is in MongoDB, an event only saves
 * the browser from asking for it.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "kafka")
public class KafkaCallBroadcaster implements CallBroadcaster {

    static final String DESTINATION = "callEvents-out-0";

    private final StreamBridge streamBridge;
    private final CallEventBroadcastMapper mapper;

    /** Events read back from the broker, for this instance's streams. Outlives its subscribers. */
    private final Sinks.Many<CallEvent> inbound =
            Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

    public KafkaCallBroadcaster(StreamBridge streamBridge, CallEventBroadcastMapper mapper) {
        this.streamBridge = streamBridge;
        this.mapper = mapper;
    }

    @Override
    public Mono<Void> broadcast(CallEvent event) {
        return Mono.fromCallable(() -> streamBridge.send(DESTINATION, mapper.toBroadcastDTO(event)))
                .doOnNext(sent -> {
                    if (!Boolean.TRUE.equals(sent)) {
                        log.error("Could not broadcast a {} call event for {}", event.type(), event.recipientId());
                    }
                })
                .then();
    }

    @Override
    public Flux<CallEvent> subscribe(String userId) {
        return inbound.asFlux().filter(event -> Objects.equals(event.recipientId(), userId));
    }

    /** Called by {@code CallEventConsumer} for each record read from the topic. */
    void accept(CallEventBroadcastDTO dto) {
        Sinks.EmitResult result = inbound.tryEmitNext(mapper.toCallEvent(dto));
        if (result.isFailure()) {
            log.error("Dropped a broadcast call event for {}: {}", dto.recipientId(), result);
        }
    }
}
