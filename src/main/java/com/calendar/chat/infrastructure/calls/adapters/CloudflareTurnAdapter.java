package com.calendar.chat.infrastructure.calls.adapters;

import com.calendar.chat.domain.models.IceServer;
import com.calendar.chat.domain.ports.IceServerProvider;
import com.calendar.chat.infrastructure.calls.mappers.RealtimeMapper;
import com.calendar.chat.infrastructure.calls.models.TurnCredentialsRequest;
import com.calendar.chat.infrastructure.calls.models.TurnCredentialsResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * Short-lived TURN credentials from Cloudflare, so a browser behind a strict NAT (often on 4G)
 * can still reach the relay. Without TURN, or when Cloudflare fails, it falls back on STUN alone:
 * most networks connect with it, and a join should not fail for the few that do not.
 */
@Slf4j
@Component
public class CloudflareTurnAdapter implements IceServerProvider {

    static final String CREDENTIALS = "/turn/keys/{keyId}/credentials/generate-ice-servers";
    /** Covers a long call; a leaked credential is useless a few hours later. */
    static final Duration TTL = Duration.ofHours(4);
    static final List<IceServer> STUN_ONLY = List.of(new IceServer(List.of("stun:stun.cloudflare.com:3478"), null, null));

    private final WebClient client;
    private final String keyId;
    private final RealtimeMapper mapper;

    public CloudflareTurnAdapter(@Qualifier("turnWebClient") WebClient client,
                                 @Value("${app.calls.turn.key-id}") String keyId, RealtimeMapper mapper) {
        this.client = client;
        this.keyId = keyId;
        this.mapper = mapper;
    }

    @Override
    public Mono<List<IceServer>> iceServers() {
        if (keyId == null || keyId.isBlank()) {
            return Mono.just(STUN_ONLY);
        }
        return Mono.defer(() -> client.post()
                        .uri(CREDENTIALS, keyId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(new TurnCredentialsRequest(TTL.toSeconds()))
                        .retrieve()
                        .bodyToMono(TurnCredentialsResponse.class))
                .filter(response -> response.iceServers() != null && !response.iceServers().isEmpty())
                .map(response -> response.iceServers().stream().map(mapper::toIceServer).toList())
                .switchIfEmpty(Mono.just(STUN_ONLY))
                .onErrorResume(error -> {
                    log.warn("No TURN credentials, STUN only: {}", error.toString());
                    return Mono.just(STUN_ONLY);
                });
    }
}
