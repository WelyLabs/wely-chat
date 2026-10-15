package com.calendar.chat.infrastructure.messaging.adapters;

import com.calendar.chat.domain.services.AccountErasureService;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.messaging.models.UserDeletedEventDTO;
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

/** Consumes {@code USER_DELETED} and erases the account, retrying a failure before logging and skipping it. */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "chat.user-deleted.enabled", havingValue = "true", matchIfMissing = true)
public class UserDeletedConsumer {

    static final int MAX_ATTEMPTS = 5;
    static final Duration FIRST_BACKOFF = Duration.ofSeconds(1);
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private final AccountErasureService erasure;
    private final ObjectMapper objectMapper;

    public UserDeletedConsumer(AccountErasureService erasure, ObjectMapper objectMapper) {
        this.erasure = erasure;
        this.objectMapper = objectMapper;
    }

    @Bean
    public Function<Flux<Message<byte[]>>, Mono<Void>> userDeleted() {
        return records -> records.concatMap(this::apply).then();
    }

    /** Read here rather than by the binder: one unreadable record must not end the stream. */
    private Mono<Void> apply(Message<byte[]> record) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.getPayload(), UserDeletedEventDTO.class))
                .onErrorResume(error -> {
                    log.error("USER_DELETED record unreadable, skipped: {}", error.getMessage());
                    return Mono.empty();
                })
                .flatMap(this::erase);
    }

    private Mono<Void> erase(UserDeletedEventDTO deleted) {
        if (deleted.userId() == null || deleted.userId().isBlank()) {
            log.error("USER_DELETED record without userId, skipped");
            return Mono.empty();
        }
        return Mono.defer(() -> erasure.erase(deleted.userId(), deleted.deletedAt()))
                // Rethrows the last failure, so the warning below names the real cause.
                .retryWhen(Retry.backoff(MAX_ATTEMPTS, FIRST_BACKOFF).maxBackoff(MAX_BACKOFF)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .onErrorResume(error -> {
                    log.warn("USER_DELETED not applied, userId={}, reconciliation will retry: {}",
                            deleted.userId(), reason(error));
                    return Mono.empty();
                });
    }

    /** A domain failure names its code, such as CHT-TEC-004; anything else its type and message. */
    private static String reason(Throwable error) {
        return error instanceof ChatException chat
                ? chat.getErrorCode().getCode() + " " + chat.getMessage()
                : error.toString();
    }
}
