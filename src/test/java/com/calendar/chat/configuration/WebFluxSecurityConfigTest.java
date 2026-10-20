package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.springSecurity;

/**
 * Covers both beans of {@link WebFluxSecurityConfig}.
 *
 * <p>This service has no exempt path: conversations and history are private, so the
 * chain is deny-by-default with only the CORS preflight let through. RSocket
 * authentication is separate — see {@code RSocketSecurityConfig}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
class WebFluxSecurityConfigTest {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private WebFluxSecurityConfig config;

    @Value("${local.server.port}")
    private int port;

    @MockitoBean
    private ReactiveJwtDecoder jwtDecoder;

    /** Mocked so this test covers security rather than MongoDB. */
    @MockitoBean
    private ChatRepository chatRepository;

    private WebTestClient client() {
        return WebTestClient.bindToApplicationContext(context)
                .apply(springSecurity())
                .configureClient()
                .responseTimeout(Duration.ofSeconds(30))
                .build();
    }

    @Test
    void apiHttpSecurity_shouldRejectUnauthenticatedConversationListing() {
        client().get().uri("/chat-service/conversations/all")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    @DisplayName("conversation history is unreachable without a token")
    void apiHttpSecurity_shouldRejectUnauthenticatedHistoryReads() {
        client().get().uri("/chat-service/conversations/conv-1/loadMessages?bucketIndex=0")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void apiHttpSecurity_shouldAllowAuthenticatedRequests() {
        when(chatRepository.findUserConversations(anyString())).thenReturn(Flux.empty());

        client().mutateWith(mockJwt().jwt(jwt -> jwt.claim("businessId", "user-1")))
                .get().uri("/chat-service/conversations/all")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    @DisplayName("the CORS preflight stays open, or the browser blocks everything")
    void apiHttpSecurity_shouldAllowCorsPreflight() {
        client().options().uri("/chat-service/conversations/all")
                .header("Origin", "http://localhost:4200")
                .header("Access-Control-Request-Method", "GET")
                .exchange()
                .expectStatus().is2xxSuccessful();
    }

    @Test
    void apiHttpSecurity_shouldDeclareNoExemptBusinessPath() {
        // No business path may be permitAll: everything here is private.
        client().get().uri("/chat-service/conversations?friendId=friend-1")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    @DisplayName("the health probes answer without a token, or the kubelet sees 401")
    void apiHttpSecurity_shouldExposeHealthProbesAnonymously() {
        // The kubelet carries no JWT. Were these paths to require authentication,
        // liveness would fail in a loop and Kubernetes would restart healthy pods.
        client().get().uri("/actuator/health/liveness")
                .exchange()
                .expectStatus().isOk();

        client().get().uri("/actuator/health/readiness")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    @DisplayName("the rest of actuator is not opened along with it")
    void apiHttpSecurity_shouldNotExposeTheRestOfActuator() {
        client().get().uri("/actuator/env")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void jwtDecoder_shouldBeConfigured() {
        assertThat(config.jwtDecoder()).isNotNull();
    }

    @Test
    @DisplayName("a request without a token answers this service's problem and keeps the Bearer challenge")
    void apiHttpSecurity_shouldAnswerAProblem_whenTheTokenIsMissing() {
        client().get().uri("/chat-service/conversations/all")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().valueMatches(HttpHeaders.WWW_AUTHENTICATE, "^Bearer.*")
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody()
                .jsonPath("$.code").isEqualTo(BusinessErrorCode.AUTHENTICATION_REQUIRED.getCode())
                .jsonPath("$.status").isEqualTo(401)
                .jsonPath("$.instance").isEqualTo("/chat-service/conversations/all")
                .jsonPath("$.requestId").value(id -> assertThat(id).asString().isNotBlank());
    }

    @Test
    @DisplayName("Prometheus scrapes without a token, and finds the technical failures counted")
    void apiHttpSecurity_shouldExposePrometheusAnonymously_withTheTechnicalErrorCounter() {
        when(chatRepository.findUserConversations(anyString()))
                .thenReturn(Flux.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));
        client().mutateWith(mockJwt().jwt(jwt -> jwt.claim("businessId", "user-1")))
                .get().uri("/chat-service/conversations/all")
                .exchange()
                .expectStatus().isEqualTo(502);

        client().get().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(metrics -> assertThat(metrics)
                        .contains("wely_errors_technical_total{code=\"CHT-TEC-004\",source=\"http\"}"));
    }

    @Test
    void apiHttpSecurity_shouldNotOpenPrometheusToWrites() {
        client().post().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    @DisplayName("Prometheus shows HTTP latency in fixed buckets, and the chat's product metrics")
    void prometheus_shouldExposeHttpLatencyBuckets_andTheProductMetrics() {
        // A real server: the HTTP observation is recorded by the server's handler, not by a context-bound client.
        WebTestClient server = WebTestClient.bindToServer().baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(30)).build();
        server.get().uri("/actuator/health/liveness").exchange().expectStatus().isOk();

        server.get().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(metrics -> assertThat(metrics)
                        .containsPattern("http_server_requests_seconds_bucket\\{[^}]*le=\"0\\.25\"")
                        // The real series names, as Grafana queries them.
                        .contains("wely_chat_messages_sent_total{kind=\"text\"}")
                        .contains("wely_chat_messages_sent_total{kind=\"photo\"}")
                        .contains("wely_chat_calls_started_total ")
                        .contains("wely_chat_calls_duration_seconds_bucket{le=\"30.0\"}")
                        .contains("wely_chat_calls_duration_seconds_bucket{le=\"3600.0\"}")
                        .contains("wely_chat_calls_duration_seconds_bucket{le=\"+Inf\"}")
                        .contains("wely_chat_calls_duration_seconds_count ")
                        .contains("wely_chat_calls_duration_seconds_sum ")
                        .contains("wely_chat_calls_duration_seconds_max ")
                        .contains("wely_chat_streams_active "));
    }
}
