package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.ports.MessageBroadcaster;
import com.calendar.chat.infrastructure.messaging.mappers.MessageBroadcastMapper;
import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
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
 * Fans messages out across every instance, through a broker.
 *
 * <h2>The part that is easy to get wrong</h2>
 *
 * <p>Kafka distributes a partition's records across the members of a consumer group: exactly
 * one member receives each record. That is what you want for work queues and exactly what
 * you do not want here — with all pods in one group, a message would reach one pod, and
 * whether that is the pod holding the recipient's stream is pure chance.
 *
 * <p>So every instance must be its own group. {@code chat.broadcast.instance-id} defaults to
 * a fresh UUID per process, which makes each pod an independent group that receives every
 * record. The consequence to accept: the topic must be short-retention and compaction-free,
 * because these groups are disposable and their offsets accumulate.
 *
 * <h2>What this does not become</h2>
 *
 * <p>Delivery is still best-effort. This is a live-update channel, not the record: the
 * message is already in MongoDB before it is broadcast, and a client that missed a frame
 * reloads the conversation. Treating the broker as the source of truth would demand
 * acknowledgements and replay, for a copy that already exists.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "kafka")
public class KafkaMessageBroadcaster implements MessageBroadcaster {

    static final String DESTINATION = "messageBroadcast-out-0";

    private final StreamBridge streamBridge;
    private final MessageBroadcastMapper mapper;

    /**
     * Messages that came back from the broker, handed to the local RSocket streams. Kafka
     * carries them between instances; this sink carries them inside one.
     *
     * <p>{@code autoCancel = false}: the default terminates the sink once its last subscriber
     * leaves, which here means the pod stops delivering consumed records for good the first
     * time every chat stream on it closes.
     */
    private final Sinks.Many<Message> inbound =
            Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

    public KafkaMessageBroadcaster(StreamBridge streamBridge, MessageBroadcastMapper mapper) {
        this.streamBridge = streamBridge;
        this.mapper = mapper;
    }

    @Override
    public Mono<Void> broadcast(Message message) {
        return Mono.fromCallable(() -> streamBridge.send(DESTINATION, mapper.toBroadcastDTO(message)))
                .doOnNext(sent -> {
                    if (!Boolean.TRUE.equals(sent)) {
                        // Logged rather than raised: the message is already persisted, so
                        // losing the live update costs a refresh, not the message.
                        log.error("Could not broadcast a message for {}", message.receiverId());
                    }
                })
                .then();
    }

    @Override
    public Flux<Message> subscribe(String userId) {
        return inbound.asFlux().filter(message -> Objects.equals(message.receiverId(), userId));
    }

    /** Called by {@code MessageBroadcastConsumer} for each record read from the topic. */
    void accept(MessageBroadcastDTO dto) {
        Sinks.EmitResult result = inbound.tryEmitNext(mapper.toMessage(dto));
        if (result.isFailure()) {
            log.error("Dropped a broadcast message for {}: {}", dto.receiverId(), result);
        }
    }
}
