package com.calendar.chat.infrastructure.calls.adapters;

import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.RelaySubscription;
import com.calendar.chat.domain.models.RemoteTrack;
import com.calendar.chat.domain.models.SessionDescription;
import com.calendar.chat.domain.ports.MediaRelay;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.calls.mappers.RealtimeMapper;
import com.calendar.chat.infrastructure.calls.models.RealtimeCloseRequest;
import com.calendar.chat.infrastructure.calls.models.RealtimeRenegotiateRequest;
import com.calendar.chat.infrastructure.calls.models.RealtimeResponse;
import com.calendar.chat.infrastructure.calls.models.RealtimeTrackResult;
import com.calendar.chat.infrastructure.calls.models.RealtimeTracksRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * The media relay on Cloudflare Realtime's SFU. Each browser has one session there; this adapter
 * creates it, publishes on it and subscribes it to others' tracks, with the app secret that only
 * this service holds. Any failure becomes CALLS_UNAVAILABLE (502).
 */
@Slf4j
@Component
public class CloudflareRealtimeAdapter implements MediaRelay {

    static final String SESSIONS = "/apps/{appId}/sessions/new";
    static final String TRACKS_NEW = "/apps/{appId}/sessions/{sessionId}/tracks/new";
    static final String TRACKS_UPDATE = "/apps/{appId}/sessions/{sessionId}/tracks/update";
    static final String TRACKS_CLOSE = "/apps/{appId}/sessions/{sessionId}/tracks/close";
    static final String RENEGOTIATE = "/apps/{appId}/sessions/{sessionId}/renegotiate";
    static final String SESSION = "/apps/{appId}/sessions/{sessionId}";
    static final String INACTIVE = "inactive";

    private final WebClient client;
    private final String appId;
    private final RealtimeMapper mapper;

    public CloudflareRealtimeAdapter(@Qualifier("realtimeWebClient") WebClient client,
                                     @Value("${app.calls.realtime.app-id}") String appId, RealtimeMapper mapper) {
        this.client = client;
        this.appId = appId;
        this.mapper = mapper;
    }

    @Override
    public Mono<String> newSession() {
        return exchange(HttpMethod.POST, SESSIONS, null, null)
                .flatMap(response -> Mono.justOrEmpty(response.sessionId()))
                .switchIfEmpty(Mono.error(unavailable()));
    }

    @Override
    public Mono<SessionDescription> publish(String sessionId, SessionDescription offer, List<LocalTrack> tracks) {
        RealtimeTracksRequest request = new RealtimeTracksRequest(mapper.toRealtime(offer),
                tracks.stream().map(mapper::toLocal).toList());
        return exchange(HttpMethod.POST, TRACKS_NEW, sessionId, request)
                .flatMap(response -> Mono.justOrEmpty(response.sessionDescription()))
                .map(mapper::toDomain)
                .switchIfEmpty(Mono.error(unavailable()));
    }

    /** A track that failed alone, typically because its publisher just left, is left out. */
    @Override
    public Mono<RelaySubscription> subscribe(String sessionId, List<RemoteTrack> tracks) {
        RealtimeTracksRequest request = new RealtimeTracksRequest(null, tracks.stream().map(mapper::toRemote).toList());
        return exchange(HttpMethod.POST, TRACKS_NEW, sessionId, request)
                .map(response -> new RelaySubscription(
                        response.sessionDescription() == null ? null : mapper.toDomain(response.sessionDescription()),
                        Boolean.TRUE.equals(response.requiresImmediateRenegotiation()),
                        response.tracks() == null ? List.of() : response.tracks().stream()
                                .filter(CloudflareRealtimeAdapter::succeeded)
                                .map(mapper::toRemoteTrack)
                                .toList()));
    }

    @Override
    public Mono<Void> renegotiate(String sessionId, SessionDescription answer) {
        return exchange(HttpMethod.PUT, RENEGOTIATE, sessionId,
                new RealtimeRenegotiateRequest(mapper.toRealtime(answer))).then();
    }

    @Override
    public Mono<Void> updateLayers(String sessionId, List<RemoteTrack> tracks) {
        return exchange(HttpMethod.PUT, TRACKS_UPDATE, sessionId,
                new RealtimeTracksRequest(null, tracks.stream().map(mapper::toRemote).toList())).then();
    }

    @Override
    public Mono<Void> closeTracks(String sessionId, List<String> mids) {
        return exchange(HttpMethod.PUT, TRACKS_CLOSE, sessionId,
                new RealtimeCloseRequest(mids.stream().map(mapper::toClosing).toList(), true)).then();
    }

    /** Realtime has no session close: every track the session still has open is closed instead. */
    @Override
    public Mono<Void> closeSession(String sessionId) {
        return exchange(HttpMethod.GET, SESSION, sessionId, null)
                .map(session -> session.tracks() == null ? List.<String>of() : session.tracks().stream()
                        .filter(track -> track.mid() != null && !INACTIVE.equals(track.status()))
                        .map(RealtimeTrackResult::mid)
                        .toList())
                .flatMap(mids -> mids.isEmpty() ? Mono.<Void>empty() : closeTracks(sessionId, mids));
    }

    private Mono<RealtimeResponse> exchange(HttpMethod method, String path, String sessionId, Object body) {
        if (appId == null || appId.isBlank()) {
            log.warn("Cloudflare Realtime is not configured: no app id");
            return Mono.error(unavailable());
        }
        return Mono.defer(() -> {
                    WebClient.RequestBodySpec request = client.method(method).uri(path, appId, sessionId)
                            .contentType(MediaType.APPLICATION_JSON);
                    WebClient.RequestHeadersSpec<?> withBody = body == null ? request : request.bodyValue(body);
                    return withBody.retrieve()
                            .onStatus(HttpStatusCode::isError, response -> response.bodyToMono(String.class)
                                    .defaultIfEmpty("")
                                    .map(detail -> {
                                        log.warn("Cloudflare Realtime answered {} on {}: {}",
                                                response.statusCode(), path, detail);
                                        return unavailable();
                                    }))
                            .bodyToMono(RealtimeResponse.class);
                })
                .flatMap(response -> {
                    if (response.errorCode() != null) {
                        log.warn("Cloudflare Realtime refused {}: {} {}", path, response.errorCode(),
                                response.errorDescription());
                        return Mono.error(unavailable());
                    }
                    return Mono.just(response);
                })
                .onErrorMap(error -> !(error instanceof ChatException), error -> {
                    log.warn("Cloudflare Realtime unreachable on {}: {}", path, error.toString());
                    return unavailable();
                });
    }

    private static boolean succeeded(RealtimeTrackResult track) {
        if (track.errorCode() != null) {
            log.info("Track {} not subscribed: {}", track.trackName(), track.errorCode());
            return false;
        }
        return true;
    }

    private static ChatException unavailable() {
        return new ChatException(ChatErrorCode.CALLS_UNAVAILABLE);
    }
}
