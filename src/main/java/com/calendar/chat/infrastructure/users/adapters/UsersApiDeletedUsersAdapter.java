package com.calendar.chat.infrastructure.users.adapters;

import com.calendar.chat.domain.models.DeletedUsersPage;
import com.calendar.chat.domain.ports.DeletedUsersSource;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.users.mappers.DeletedUsersMapper;
import com.calendar.chat.infrastructure.users.models.DeletedUsersPageResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

/** Reads wely-users' tombstones from its internal listing; every failure becomes USERS_UNAVAILABLE (502). */
@Slf4j
@Component
public class UsersApiDeletedUsersAdapter implements DeletedUsersSource {

    static final String DELETED_USERS_PATH = "/user-service/internal/deleted-users";
    static final String LIMIT_PARAM = "limit";
    static final String AFTER_PARAM = "after";
    /** The whole call, body included; the client's own timeouts only bound each network step. */
    static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final WebClient client;
    private final DeletedUsersMapper mapper;
    private final boolean configured;

    public UsersApiDeletedUsersAdapter(@Qualifier("usersWebClient") WebClient client, DeletedUsersMapper mapper,
                                       @Value("${app.users-api.url:}") String usersApiUrl,
                                       @Value("${app.users-api.deleted-users-secret:}") String secret) {
        this.client = client;
        this.mapper = mapper;
        this.configured = !usersApiUrl.isBlank() && !secret.isBlank();
    }

    @Override
    public Mono<DeletedUsersPage> page(String after, int limit) {
        if (!configured) {
            log.warn("Deleted accounts not listed: USERS_API_URL or DELETED_USERS_READER_SECRET is not set");
            return Mono.error(new ChatException(ChatErrorCode.USERS_UNAVAILABLE));
        }
        return Mono.defer(() -> client.get()
                        .uri(uri -> {
                            uri.path(DELETED_USERS_PATH).queryParam(LIMIT_PARAM, limit);
                            // The cursor goes through the URI template, so it is encoded whatever it holds.
                            return after == null ? uri.build() : uri.queryParam(AFTER_PARAM, "{after}").build(after);
                        })
                        .retrieve()
                        .bodyToMono(DeletedUsersPageResponse.class))
                .timeout(TIMEOUT)
                // A body without items is a broken answer, not an empty listing: only {"items":[]} ends a pass clean.
                .filter(response -> response.items() != null)
                .switchIfEmpty(Mono.error(() -> new IllegalStateException("wely-users answered without items")))
                .map(mapper::toPage)
                .onErrorMap(UsersApiDeletedUsersAdapter::isUnexpected, UsersApiDeletedUsersAdapter::unavailable);
    }

    // The error names the status and the URL, never the secret header.
    private static ChatException unavailable(Throwable error) {
        log.warn("Deleted accounts not listed from wely-users: {}", error.toString());
        return new ChatException(ChatErrorCode.USERS_UNAVAILABLE);
    }

    private static boolean isUnexpected(Throwable error) {
        return !(error instanceof ChatException);
    }
}
