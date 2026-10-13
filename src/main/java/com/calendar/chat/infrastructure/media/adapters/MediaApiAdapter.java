package com.calendar.chat.infrastructure.media.adapters;

import com.calendar.chat.domain.models.MediaProfile;
import com.calendar.chat.domain.models.StoredMedia;
import com.calendar.chat.domain.ports.MediaStore;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.media.mappers.MediaMapper;
import com.calendar.chat.infrastructure.media.models.MediaProblem;
import com.calendar.chat.infrastructure.media.models.SignUrlsRequest;
import com.calendar.chat.infrastructure.media.models.SignedUrlsResponse;
import com.calendar.chat.infrastructure.media.models.StoredMediaResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

/**
 * Stores, removes and signs photos through wely-media's internal API, which owns the bucket.
 * An invalid or oversized image keeps its own code; anything else becomes MEDIA_UNAVAILABLE (502).
 */
@Component
public class MediaApiAdapter implements MediaStore {

    static final String MEDIA_PATH = "/internal/media";
    static final String URLS_PATH = "/internal/media/urls";
    /** Long enough to scroll a conversation, short enough that a shared link stops working. */
    static final long URL_TTL_SECONDS = 3600;
    /** wely-media wants a file name on the part; the key already names the object. */
    static final String UPLOAD_FILENAME = "upload";
    static final String INVALID_IMAGE = "MED-BUS-001";
    static final String IMAGE_TOO_LARGE = "MED-BUS-002";

    private final WebClient client;
    private final MediaMapper mapper;

    public MediaApiAdapter(@Qualifier("mediaWebClient") WebClient client, MediaMapper mapper) {
        this.client = client;
        this.mapper = mapper;
    }

    @Override
    public Mono<StoredMedia> store(String key, byte[] content, MediaProfile profile) {
        MultipartBodyBuilder parts = new MultipartBodyBuilder();
        parts.part("file", content, MediaType.APPLICATION_OCTET_STREAM).filename(UPLOAD_FILENAME);
        parts.part("key", key);
        parts.part("profile", profile.name());
        return Mono.defer(() -> client.post()
                        .uri(MEDIA_PATH)
                        .contentType(MediaType.MULTIPART_FORM_DATA)
                        .body(BodyInserters.fromMultipartData(parts.build()))
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, MediaApiAdapter::toChatException)
                        .bodyToMono(StoredMediaResponse.class))
                .map(mapper::toStoredMedia)
                .onErrorMap(MediaApiAdapter::isUnexpected, error -> new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE));
    }

    /** wely-media answers 204 for a missing key too, so deleting twice is not an error. */
    @Override
    public Mono<Void> delete(String key) {
        return Mono.defer(() -> client.delete()
                        .uri(uri -> uri.path(MEDIA_PATH).queryParam("key", "{key}").build(key))
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, MediaApiAdapter::toChatException)
                        .toBodilessEntity()
                        .then())
                .onErrorMap(MediaApiAdapter::isUnexpected, error -> new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE));
    }

    /** One call: the caller keeps to 200 keys. */
    @Override
    public Mono<Map<String, String>> urls(Collection<String> keys) {
        return Mono.defer(() -> client.post()
                        .uri(URLS_PATH)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(new SignUrlsRequest(keys, URL_TTL_SECONDS))
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, MediaApiAdapter::toChatException)
                        .bodyToMono(SignedUrlsResponse.class))
                .map(response -> response.urls() == null ? Map.<String, String>of() : response.urls())
                .defaultIfEmpty(Map.of())
                .onErrorMap(MediaApiAdapter::isUnexpected, error -> new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE));
    }

    /** wely-media's code decides: a refused image is the client's fault, the rest is ours. */
    private static Mono<ChatException> toChatException(ClientResponse response) {
        return response.bodyToMono(MediaProblem.class)
                .map(problem -> errorCodeOf(problem.code()))
                .onErrorReturn(ChatErrorCode.MEDIA_UNAVAILABLE)
                .defaultIfEmpty(ChatErrorCode.MEDIA_UNAVAILABLE)
                .map(ChatException::new);
    }

    static ChatErrorCode errorCodeOf(String mediaCode) {
        return switch (mediaCode == null ? "" : mediaCode) {
            case INVALID_IMAGE -> ChatErrorCode.PHOTO_INVALID;
            case IMAGE_TOO_LARGE -> ChatErrorCode.PHOTO_TOO_LARGE;
            default -> ChatErrorCode.MEDIA_UNAVAILABLE;
        };
    }

    private static boolean isUnexpected(Throwable error) {
        return !(error instanceof ChatException);
    }
}
