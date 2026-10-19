package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.models.Friendship;
import com.calendar.chat.domain.services.FriendshipService;
import com.calendar.chat.infrastructure.messaging.models.FriendshipStateDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * Consumes wely-social's compacted {@code FRIENDSHIP_STATE}, keeping the {@code friendships} projection: a value
 * is the pair while they are friends, a tombstone (null value) ends the friendship its key names. Compaction
 * keeps the latest record per key, so a new consumer group rebuilds the projection from the earliest offset.
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.friendship-state.enabled", havingValue = "true", matchIfMissing = true)
public class FriendshipStateConsumer {

    static final String TOPIC = "FRIENDSHIP_STATE";
    static final int MAX_ATTEMPTS = 5;
    static final Duration FIRST_BACKOFF = Duration.ofSeconds(1);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    private static final String KEY_SEPARATOR = ":";

    private final FriendshipService friendshipService;
    private final ObjectMapper objectMapper;

    public FriendshipStateConsumer(FriendshipService friendshipService, ObjectMapper objectMapper) {
        this.friendshipService = friendshipService;
        this.objectMapper = objectMapper;
    }

    @Bean
    public Function<Flux<Message<byte[]>>, Mono<Void>> friendshipState() {
        return records -> records.concatMap(this::handle).then();
    }

    // Typed Message<?>: Spring Cloud Function hands a tombstone over with a KafkaNull payload, not bytes.
    private Mono<Void> handle(Message<?> record) {
        Optional<String[]> key = pairOfKey(record.getHeaders().get(KafkaHeaders.RECEIVED_KEY));
        Object payload = record.getPayload();
        if (payload == null || payload instanceof KafkaNull) {
            return key.map(pair -> withRetries(Mono.defer(() -> friendshipService.applyTombstone(pair[0], pair[1])),
                            Friendship.pairIdOf(pair[0], pair[1]), "tombstone ended the friendship"))
                    .orElseGet(() -> {
                        log.error("{} tombstone dropped: its key is not two distinct UUIDs joined by ':'", TOPIC);
                        return Mono.empty();
                    });
        }
        return Mono.fromCallable(() -> objectMapper.readValue((byte[]) payload, FriendshipStateDTO.class))
                .flatMap(dto -> state(dto)
                        .filter(state -> key.isEmpty() || state.pairId().equals(Friendship.pairIdOf(key.get()[0], key.get()[1])))
                        .map(state -> withRetries(Mono.defer(() -> friendshipService.applyState(state)),
                                state.pairId(), "applied, version=" + state.version()))
                        .orElseGet(() -> {
                            log.error("{} record dropped: needs two distinct UUIDs matching the key, friends, and a version",
                                    TOPIC);
                            return Mono.empty();
                        }))
                .onErrorResume(error -> {
                    log.error("{} record unreadable, dropped: {}", TOPIC, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> withRetries(Mono<Boolean> step, String pairId, String applied) {
        return step
                // Rethrows the last failure, so the warning below names the real cause.
                .retryWhen(Retry.backoff(MAX_ATTEMPTS, FIRST_BACKOFF).maxBackoff(MAX_BACKOFF)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .doOnNext(done -> {
                    if (done) {
                        log.info("{} {}, pair={}", TOPIC, applied, pairId);
                    } else {
                        log.debug("{} ignored, stale, already applied or naming a deleted account, pair={}", TOPIC, pairId);
                    }
                })
                .then()
                .onErrorResume(error -> {
                    log.warn("{} not applied, pair={}: {}", TOPIC, pairId, error.toString());
                    return Mono.empty();
                });
    }

    /** The pair a value describes; empty for anything malformed, or for a pair that is not friends. */
    static Optional<Friendship> state(FriendshipStateDTO dto) {
        Optional<String> a = canonical(dto.a());
        Optional<String> b = canonical(dto.b());
        if (!Boolean.TRUE.equals(dto.friends()) || dto.version() == null || a.isEmpty() || b.isEmpty()
                || a.get().equals(b.get())) {
            return Optional.empty();
        }
        return Optional.of(new Friendship(a.get(), b.get(), dto.version()));
    }

    /** The two ids of an {@code a:b} key, bytes by default, a string with a string deserializer. */
    static Optional<String[]> pairOfKey(Object key) {
        String text = key instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8)
                : key instanceof String string ? string : null;
        if (text == null) {
            return Optional.empty();
        }
        String[] parts = text.split(KEY_SEPARATOR, -1);
        if (parts.length != 2) {
            return Optional.empty();
        }
        Optional<String> a = canonical(parts[0]);
        Optional<String> b = canonical(parts[1]);
        if (a.isEmpty() || b.isEmpty() || a.get().equals(b.get())) {
            return Optional.empty();
        }
        return Optional.of(new String[] {a.get(), b.get()});
    }

    /** The id in the lower-case form wely-users issues; empty when it is not a UUID. */
    static Optional<String> canonical(String id) {
        if (id == null) {
            return Optional.empty();
        }
        try {
            String canonical = UUID.fromString(id).toString();
            return canonical.equalsIgnoreCase(id) ? Optional.of(canonical) : Optional.empty();
        } catch (IllegalArgumentException notAUuid) {
            return Optional.empty();
        }
    }
}
