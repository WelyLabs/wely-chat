package com.calendar.chat.infrastructure.media.adapters;

import com.calendar.chat.domain.ports.PhotoStorage;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.media.signing.GarageRequestSigner;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/**
 * Stores pictures in a private Garage bucket through its S3 API, with plain HTTP calls signed by
 * {@link GarageRequestSigner}. A failure reaching Garage becomes MEDIA_UNAVAILABLE (502).
 *
 * <p>Two endpoints, on purpose. Writes go to the internal Service. Read URLs are signed for the
 * public hostname, because a signature covers the host: one signed for the internal name would be
 * refused when the browser presents it through the tunnel.
 *
 * <p>Every object, and every signed read, says {@code Cache-Control: private}: the hostname sits
 * behind Cloudflare, and a shared cache keeping a private picture would hand it to whoever asks
 * next with any URL that maps to the same cache entry.
 */
@Component
public class GaragePhotoStorageAdapter implements PhotoStorage {

    static final String CACHE_CONTROL = "private, max-age=3600";
    /** A write is signed just before it is sent: a minute is ample and leaves nothing reusable. */
    static final Duration WRITE_TTL = Duration.ofMinutes(1);

    private final WebClient client;
    private final GarageRequestSigner signer;
    private final URI internalEndpoint;
    private final URI publicEndpoint;
    private final String bucket;

    public GaragePhotoStorageAdapter(@Qualifier("mediaWebClient") WebClient client,
                                       GarageRequestSigner signer,
                                       @Value("${app.media.internal-endpoint}") URI internalEndpoint,
                                       @Value("${app.media.public-endpoint}") URI publicEndpoint,
                                       @Value("${app.media.bucket}") String bucket) {
        this.client = client;
        this.signer = signer;
        this.internalEndpoint = internalEndpoint;
        this.publicEndpoint = publicEndpoint;
        this.bucket = bucket;
    }

    @Override
    public Mono<Void> put(String key, byte[] content, String contentType) {
        return Mono.defer(() -> client.put()
                .uri(signer.presign(HttpMethod.PUT.name(), internalEndpoint, path(key), WRITE_TTL, Map.of()))
                .header(HttpHeaders.CONTENT_TYPE, contentType)
                .header(HttpHeaders.CACHE_CONTROL, CACHE_CONTROL)
                .bodyValue(content)
                .retrieve()
                .toBodilessEntity()
                .then())
                .onErrorMap(error -> !(error instanceof ChatException), error -> new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE));
    }

    /** Garage answers 204 for a missing key too, so deleting twice is not an error. */
    @Override
    public Mono<Void> delete(String key) {
        return Mono.defer(() -> client.delete()
                .uri(signer.presign(HttpMethod.DELETE.name(), internalEndpoint, path(key), WRITE_TTL, Map.of()))
                .retrieve()
                .toBodilessEntity()
                .then())
                .onErrorMap(error -> !(error instanceof ChatException), error -> new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE));
    }

    @Override
    public String signedUrl(String key, Duration ttl) {
        return signer.presign(HttpMethod.GET.name(), publicEndpoint, path(key), ttl,
                        Map.of("response-cache-control", CACHE_CONTROL))
                .toString();
    }

    /** Path-style addressing: there is no wildcard hostname per bucket. */
    private String path(String key) {
        return "/" + bucket + "/" + key;
    }
}
