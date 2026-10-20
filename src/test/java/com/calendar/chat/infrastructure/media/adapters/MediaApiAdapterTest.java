package com.calendar.chat.infrastructure.media.adapters;

import com.calendar.chat.configuration.MediaApiClientConfig;
import com.calendar.chat.domain.models.MediaProfile;
import com.calendar.chat.domain.models.StoredMedia;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import com.calendar.chat.infrastructure.media.mappers.MediaMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mapstruct.factory.Mappers;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.reactive.MockClientHttpRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Requests go to a stub exchange: what is checked is what the adapter sends and how it reads the answer. */
class MediaApiAdapterTest {

    private static final String SECRET = "s3cret";

    private final List<MockClientHttpRequest> sent = new ArrayList<>();
    private ClientResponse answer;
    private MediaApiAdapter adapter;

    @BeforeEach
    void setUp() {
        answer = json(HttpStatus.OK, "{}");
        WebClient client = WebClient.builder()
                .baseUrl("http://wely-media:8085")
                .defaultHeader("X-Internal-Secret", SECRET)
                .exchangeFunction(request -> {
                    // Written out, so the test reads the body as wely-media would receive it.
                    MockClientHttpRequest written = new MockClientHttpRequest(request.method(), request.url());
                    sent.add(written);
                    return request.writeTo(written, ExchangeStrategies.withDefaults()).then(Mono.fromSupplier(() -> answer));
                })
                .build();
        adapter = new MediaApiAdapter(client, Mappers.getMapper(MediaMapper.class));
    }

    private static ClientResponse json(HttpStatus status, String body) {
        return ClientResponse.create(status)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build();
    }

    private static ClientResponse problem(HttpStatus status, String code) {
        return ClientResponse.create(status)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_PROBLEM_JSON_VALUE)
                .body("{\"type\":\"about:blank\",\"title\":\"x\",\"status\":" + status.value() + ",\"code\":\"" + code + "\"}")
                .build();
    }

    private static boolean failsWith(Throwable error, BusinessErrorCode code) {
        return error instanceof BusinessException business && business.getErrorCode() == code;
    }

    private static boolean failsWith(Throwable error, TechnicalErrorCode code) {
        return error instanceof TechnicalException technical && technical.getErrorCode() == code;
    }

    /** The name of the code the error carries, whichever of the two kinds it is. */
    private static String codeOf(Throwable error) {
        if (error instanceof BusinessException business) {
            return business.getErrorCode().name();
        }
        return error instanceof TechnicalException technical ? technical.getErrorCode().name() : null;
    }

    @Test
    void store_shouldPostTheUploadAsMultipart_withKeyProfileAndSecret() {
        answer = json(HttpStatus.CREATED,
                "{\"key\":\"chat/c1/m1.jpg\",\"width\":1600,\"height\":1200,\"contentType\":\"image/jpeg\"}");

        StepVerifier.create(adapter.store("chat/c1/m1.jpg", "JPEGDATA".getBytes(), MediaProfile.PHOTO))
                .expectNext(new StoredMedia("chat/c1/m1.jpg", 1600, 1200))
                .expectComplete().verify(Duration.ofSeconds(10));

        MockClientHttpRequest request = sent.getFirst();
        assertThat(request.getMethod()).isEqualTo(HttpMethod.POST);
        assertThat(request.getURI().toString()).isEqualTo("http://wely-media:8085/internal/media");
        assertThat(request.getHeaders().getFirst("X-Internal-Secret")).isEqualTo(SECRET);
        assertThat(request.getHeaders().getContentType().isCompatibleWith(MediaType.MULTIPART_FORM_DATA)).isTrue();
        String body = request.getBodyAsString().block();
        assertThat(body)
                .contains("name=\"file\"; filename=\"upload\"")
                .contains("JPEGDATA")
                .contains("name=\"key\"")
                .contains("chat/c1/m1.jpg")
                .contains("name=\"profile\"")
                .contains("PHOTO");
    }

    @Test
    void store_shouldSendNothing_untilSubscribed() {
        adapter.store("chat/c1/m1.jpg", new byte[] {1}, MediaProfile.PHOTO);

        assertThat(sent).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
            "BAD_REQUEST, MED-BUS-001, PHOTO_INVALID",
            "CONTENT_TOO_LARGE, MED-BUS-002, PHOTO_TOO_LARGE",
            "FORBIDDEN, MED-BUS-003, MEDIA_UNAVAILABLE",
            "UNAUTHORIZED, MED-REQ-001, MEDIA_UNAVAILABLE",
            "BAD_GATEWAY, MED-TEC-001, MEDIA_UNAVAILABLE"
    })
    void store_shouldTranslateTheMediaServicesErrorCode(HttpStatus status, String mediaCode, String expected) {
        answer = problem(status, mediaCode);

        StepVerifier.create(adapter.store("chat/c1/m1.jpg", new byte[] {1}, MediaProfile.PHOTO))
                .expectErrorMatches(error -> expected.equals(codeOf(error)))
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void store_shouldAnswerMediaUnavailable_whenTheErrorHasNoReadableBody() {
        answer = ClientResponse.create(HttpStatus.INTERNAL_SERVER_ERROR).body("<html>oops</html>").build();

        StepVerifier.create(adapter.store("chat/c1/m1.jpg", new byte[] {1}, MediaProfile.PHOTO))
                .expectErrorMatches(error -> failsWith(error, TechnicalErrorCode.MEDIA_UNAVAILABLE))
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void store_shouldAnswerMediaUnavailable_whenTheServiceCannotBeReached() {
        IllegalStateException refused = new IllegalStateException("connection refused");
        WebClient unreachable = WebClient.builder()
                .exchangeFunction(request -> Mono.error(refused))
                .build();
        MediaApiAdapter offline = new MediaApiAdapter(unreachable, Mappers.getMapper(MediaMapper.class));

        StepVerifier.create(offline.store("chat/c1/m1.jpg", new byte[] {1}, MediaProfile.PHOTO))
                .consumeErrorWith(error -> {
                    assertThat(failsWith(error, TechnicalErrorCode.MEDIA_UNAVAILABLE)).isTrue();
                    // Kept as the cause, so the ERROR log shows why wely-media could not be reached.
                    assertThat(error).hasRootCause(refused);
                }).verify(Duration.ofSeconds(10));
    }

    @Test
    void store_shouldKeepTheUnreadableBodyAsTheCause() {
        answer = ClientResponse.create(HttpStatus.INTERNAL_SERVER_ERROR).body("<html>oops</html>").build();

        StepVerifier.create(adapter.store("chat/c1/m1.jpg", new byte[] {1}, MediaProfile.PHOTO))
                .consumeErrorWith(error -> assertThat(error.getCause()).isNotNull()).verify(Duration.ofSeconds(10));
    }

    @Test
    void delete_shouldDeleteByKey_withTheSecret() {
        answer = ClientResponse.create(HttpStatus.NO_CONTENT).build();

        StepVerifier.create(adapter.delete("chat/c1/m1.jpg")).expectComplete().verify(Duration.ofSeconds(10));

        MockClientHttpRequest request = sent.getFirst();
        assertThat(request.getMethod()).isEqualTo(HttpMethod.DELETE);
        assertThat(request.getURI().getPath()).isEqualTo("/internal/media");
        assertThat(request.getURI().getQuery()).isEqualTo("key=chat/c1/m1.jpg");
        assertThat(request.getHeaders().getFirst("X-Internal-Secret")).isEqualTo(SECRET);
    }

    @Test
    void delete_shouldAnswerMediaUnavailable_whenTheServiceFails() {
        answer = problem(HttpStatus.BAD_GATEWAY, "MED-TEC-001");

        StepVerifier.create(adapter.delete("chat/c1/m1.jpg"))
                .expectErrorMatches(error -> failsWith(error, TechnicalErrorCode.MEDIA_UNAVAILABLE))
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void urls_shouldPostTheKeysAndTtl_andReturnTheUrlsByKey() {
        answer = json(HttpStatus.OK, "{\"urls\":{\"chat/c1/m1.jpg\":\"https://media/m1\",\"chat/c1/m2.jpg\":\"https://media/m2\"}}");

        StepVerifier.create(adapter.urls(List.of("chat/c1/m1.jpg", "chat/c1/m2.jpg")))
                .expectNext(Map.of("chat/c1/m1.jpg", "https://media/m1", "chat/c1/m2.jpg", "https://media/m2"))
                .expectComplete().verify(Duration.ofSeconds(10));

        MockClientHttpRequest request = sent.getFirst();
        assertThat(request.getMethod()).isEqualTo(HttpMethod.POST);
        assertThat(request.getURI().getPath()).isEqualTo("/internal/media/urls");
        assertThat(request.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
        assertThat(request.getHeaders().getFirst("X-Internal-Secret")).isEqualTo(SECRET);
        assertThat(request.getBodyAsString().block())
                .isEqualTo("{\"keys\":[\"chat/c1/m1.jpg\",\"chat/c1/m2.jpg\"],\"ttlSeconds\":3600}");
    }

    @Test
    void urls_shouldAnswerAnEmptyMap_whenTheServiceSendsNoUrls() {
        answer = json(HttpStatus.OK, "{}");

        StepVerifier.create(adapter.urls(List.of("chat/c1/m1.jpg"))).expectNext(Map.of()).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void urls_shouldAnswerMediaUnavailable_whenTheKeyIsOutsideTheChatsPrefix() {
        answer = problem(HttpStatus.FORBIDDEN, "MED-BUS-003");

        StepVerifier.create(adapter.urls(List.of("users/u1.jpg")))
                .expectErrorMatches(error -> failsWith(error, TechnicalErrorCode.MEDIA_UNAVAILABLE))
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void exceptionOf_shouldTreatAMissingCodeAsUnavailable() {
        assertThat(failsWith(MediaApiAdapter.exceptionOf(null), TechnicalErrorCode.MEDIA_UNAVAILABLE)).isTrue();
    }

    @Test
    void exceptionOf_shouldKeepARefusedImageABusinessFailure() {
        assertThat(failsWith(MediaApiAdapter.exceptionOf("MED-BUS-001"), BusinessErrorCode.PHOTO_INVALID)).isTrue();
        assertThat(failsWith(MediaApiAdapter.exceptionOf("MED-BUS-002"), BusinessErrorCode.PHOTO_TOO_LARGE)).isTrue();
    }

    private static String stackTraceOf(Throwable failure) {
        java.io.StringWriter trace = new java.io.StringWriter();
        failure.printStackTrace(new java.io.PrintWriter(trace));
        return trace.toString();
    }

    @Test
    @DisplayName("a real failed call, kept as the cause, never prints the internal secret: it travels in a header")
    void store_shouldNotLogTheInternalSecret_ofARealFailedCall() {
        WebClient client = new MediaApiClientConfig().mediaWebClient("http://unreachable.invalid:8085", SECRET);
        MediaApiAdapter real = new MediaApiAdapter(client, Mappers.getMapper(MediaMapper.class));

        StepVerifier.create(real.store("chat/c1/m1.jpg", new byte[] {1}, MediaProfile.PHOTO))
                .consumeErrorWith(error -> assertThat(stackTraceOf(error))
                        .contains("unreachable.invalid").doesNotContain(SECRET))
                .verify(java.time.Duration.ofSeconds(10));
    }
}
