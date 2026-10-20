package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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
                        .contains("wely_errors_technical_total{code=\"CHT-TEC-004\"}"));
    }

    @Test
    void apiHttpSecurity_shouldNotOpenPrometheusToWrites() {
        client().post().uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isUnauthorized();
    }
}
