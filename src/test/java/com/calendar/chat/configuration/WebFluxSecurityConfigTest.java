package com.calendar.chat.configuration;

import com.calendar.chat.domain.ports.ChatRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

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

    /** Mocké pour que ce test porte sur la sécurité et non sur MongoDB. */
    @MockitoBean
    private ChatRepository chatRepository;

    private WebTestClient client() {
        return WebTestClient.bindToApplicationContext(context)
                .apply(springSecurity())
                .configureClient()
                .build();
    }

    @Test
    void apiHttpSecurity_shouldRejectUnauthenticatedConversationListing() {
        client().get().uri("/chat-service/conversations/all")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    @DisplayName("l'historique d'une conversation n'est pas atteignable sans token")
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
    @DisplayName("le préflight CORS reste ouvert, sinon le navigateur bloque tout")
    void apiHttpSecurity_shouldAllowCorsPreflight() {
        client().options().uri("/chat-service/conversations/all")
                .header("Origin", "http://localhost:4200")
                .header("Access-Control-Request-Method", "GET")
                .exchange()
                .expectStatus().is2xxSuccessful();
    }

    @Test
    void apiHttpSecurity_shouldDeclareNoExemptBusinessPath() {
        // Aucun chemin métier ne doit être en permitAll : tout est privé ici.
        client().get().uri("/chat-service/conversations?friendId=friend-1")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    @DisplayName("les probes de santé répondent sans token, sinon le kubelet voit 401")
    void apiHttpSecurity_shouldExposeHealthProbesAnonymously() {
        // Le kubelet ne porte pas de JWT. Si ces chemins exigeaient une authentification,
        // la liveness échouerait en boucle et Kubernetes redémarrerait des pods sains.
        client().get().uri("/actuator/health/liveness")
                .exchange()
                .expectStatus().isOk();

        client().get().uri("/actuator/health/readiness")
                .exchange()
                .expectStatus().isOk();
    }

    @Test
    @DisplayName("le reste d'actuator n'est pas ouvert pour autant")
    void apiHttpSecurity_shouldNotExposeTheRestOfActuator() {
        client().get().uri("/actuator/env")
                .exchange()
                .expectStatus().isUnauthorized();
    }

    @Test
    void jwtDecoder_shouldBeConfigured() {
        assertThat(config.jwtDecoder()).isNotNull();
    }
}
