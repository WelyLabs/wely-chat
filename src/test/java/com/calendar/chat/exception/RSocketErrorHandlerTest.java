package com.calendar.chat.exception;

import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.services.LiveSessions;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.metadata.WellKnownMimeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.handler.DestinationPatternsMessageCondition;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.rsocket.metadata.BearerTokenAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.BearerTokenMetadata;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.SimpleRouteMatcher;
import org.springframework.util.AntPathMatcher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class RSocketErrorHandlerTest {

    private static final ObjectMapper JSON = JsonMapper.builder().build();
    private static final String CLIENT_ID = "abcd1234-0000-4000-8000-00000000000a";

    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final RSocketErrorHandler handler = new RSocketErrorHandler(new TechnicalErrorCounter(meterRegistry), JSON);

    private static JsonNode problemOf(Throwable frame) {
        assertThat(frame).isInstanceOf(ApplicationErrorException.class);
        return JSON.readTree(frame.getMessage());
    }

    private double technicalErrors(String code) {
        var counter = meterRegistry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, GlobalErrorHandler.SOURCE_RSOCKET)
                .counter();
        return counter == null ? 0 : counter.count();
    }

    @Test
    void toFrame_shouldCarryABusinessFailure_asAJsonProblem_withoutCountingIt() {
        JsonNode problem = problemOf(handler.toFrame(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED),
                "chat.send", CLIENT_ID));

        assertThat(problem.get("type").asString()).isEqualTo("https://welylabs.app/problems/cht-bus-011");
        assertThat(problem.get("title").asString()).isEqualTo("Account deleted");
        assertThat(problem.get("status").asInt()).isEqualTo(410);
        assertThat(problem.get("detail").asString()).isEqualTo(BusinessErrorCode.ACCOUNT_DELETED.getDetail());
        assertThat(problem.get("code").asString()).isEqualTo("CHT-BUS-011");
        assertThat(problem.get("timestamp").asString()).isNotBlank();
        assertThat(problem.get("requestId").asString()).isEqualTo(CLIENT_ID);
        assertThat(meterRegistry.getMeters()).isEmpty();
    }

    @Test
    void toFrame_shouldCarryATechnicalFailure_andCountIt_asRSocket() {
        JsonNode problem = problemOf(handler.toFrame(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE),
                "chat.send", CLIENT_ID));

        assertThat(problem.get("code").asString()).isEqualTo("CHT-TEC-004");
        assertThat(problem.get("status").asInt()).isEqualTo(502);
        assertThat(technicalErrors("CHT-TEC-004")).isEqualTo(1);
    }

    @Test
    @DisplayName("anything else is CHT-TEC-000, with a detail that leaks nothing of the cause")
    void toFrame_shouldCarryAnythingElse_asUnexpected() {
        JsonNode problem = problemOf(handler.toFrame(new IllegalStateException("mongodb://admin:hunter2@host"),
                "chat.send", CLIENT_ID));

        assertThat(problem.get("code").asString()).isEqualTo(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE);
        assertThat(problem.get("status").asInt()).isEqualTo(500);
        assertThat(problem.toString()).doesNotContain("hunter2");
        assertThat(technicalErrors(GlobalErrorHandler.UNEXPECTED_FAILURE_CODE)).isEqualTo(1);
    }

    @Test
    void toFrame_shouldPassAnAlreadyFramedError_through() {
        ApplicationErrorException framed = new ApplicationErrorException("{\"code\":\"CHT-BUS-001\"}");

        assertThat(handler.toFrame(framed, "chat.send", CLIENT_ID)).isSameAs(framed);
        assertThat(meterRegistry.getMeters()).isEmpty();
    }

    @Test
    void handle_shouldFailWithTheFrame_forTheRouteAndTheClientsId() {
        Map<String, Object> headers = Map.of(
                RequestIds.REQUEST_ID_ATTRIBUTE, CLIENT_ID,
                DestinationPatternsMessageCondition.LOOKUP_DESTINATION_HEADER,
                new SimpleRouteMatcher(new AntPathMatcher(".")).parseRoute("chat.send"));

        StepVerifier.create(handler.handle(new BusinessException(BusinessErrorCode.FRIENDSHIP_REQUIRED), headers))
                .consumeErrorWith(frame -> {
                    assertThat(problemOf(frame).get("code").asString()).isEqualTo("CHT-BUS-013");
                    assertThat(problemOf(frame).get("requestId").asString()).isEqualTo(CLIENT_ID);
                })
                .verify(Duration.ofSeconds(10));
    }

    @Test
    void requestIdOf_shouldKeepAValidClientId() {
        assertThat(RSocketErrorHandler.requestIdOf(Map.of(RequestIds.REQUEST_ID_ATTRIBUTE, CLIENT_ID))).isEqualTo(CLIENT_ID);
    }

    @Test
    @DisplayName("no id, or one that could forge a log line: a new one is generated")
    void requestIdOf_shouldGenerateOne_whenNoneOrAnInvalidOneWasSent() {
        assertThat(RSocketErrorHandler.requestIdOf(Map.of())).matches("[0-9a-f-]{36}");
        assertThat(RSocketErrorHandler.requestIdOf(Map.of(RequestIds.REQUEST_ID_ATTRIBUTE, "bad\nid"))).matches("[0-9a-f-]{36}");
    }

    /** A real RSocket requester, over WebSocket, against the application. */
    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
    @ActiveProfiles("test")
    class InTheApplication {

        private static final String TOKEN = "test-token";
        private static final MimeType AUTHENTICATION =
                MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());

        @Value("${local.server.port}")
        private int port;

        @Autowired
        private RSocketStrategies strategies;

        @Autowired
        private MeterRegistry registry;

        @MockitoBean
        private ReactiveJwtDecoder jwtDecoder;

        @MockitoBean
        private ChatService chatService;

        @MockitoBean
        private LiveSessions liveSessions;

        private RSocketRequester requester;

        private RSocketRequester requester() {
            Jwt jwt = Jwt.withTokenValue(TOKEN).header("alg", "none")
                    .claim("businessId", "user-1").claim("preferred_username", "alice").build();
            when(jwtDecoder.decode(anyString())).thenReturn(Mono.just(jwt));
            requester = RSocketRequester.builder()
                    .rsocketStrategies(strategies.mutate().encoder(new BearerTokenAuthenticationEncoder()).build())
                    .dataMimeType(MimeTypeUtils.APPLICATION_JSON)
                    .websocket(URI.create("ws://localhost:" + port + "/rsocket"));
            return requester;
        }

        @AfterEach
        void close() {
            if (requester != null) {
                requester.dispose();
            }
        }

        private Mono<String> send() {
            return requester().route("chat.send")
                    .metadata(new BearerTokenMetadata(TOKEN), AUTHENTICATION)
                    .metadata(CLIENT_ID, RequestIds.REQUEST_ID_MIME_TYPE)
                    .data(Map.of("receiverId", "user-2", "conversationId", "conv-1", "content", "hi"))
                    .retrieveMono(String.class);
        }

        private double counted(String code) {
            var counter = registry.find(GlobalErrorHandler.TECHNICAL_ERRORS_METRIC)
                    .tags(GlobalErrorHandler.CODE_TAG, code, GlobalErrorHandler.SOURCE_TAG, GlobalErrorHandler.SOURCE_RSOCKET)
                    .counter();
            return counter == null ? 0 : counter.count();
        }

        @Test
        @DisplayName("a business failure the route's Mono emits reaches the client as a JSON problem frame")
        void chatSend_shouldAnswerABusinessFailure_asAJsonProblemFrame() {
            when(chatService.sendMessage(any())).thenReturn(Mono.error(new BusinessException(BusinessErrorCode.FRIENDSHIP_REQUIRED)));

            StepVerifier.create(send())
                    .consumeErrorWith(frame -> {
                        JsonNode problem = problemOf(frame);
                        assertThat(problem.get("code").asString()).isEqualTo("CHT-BUS-013");
                        assertThat(problem.get("status").asInt()).isEqualTo(403);
                        assertThat(problem.get("requestId").asString()).isEqualTo(CLIENT_ID);
                    })
                    .verify(Duration.ofSeconds(10));
        }

        @Test
        @DisplayName("a technical failure reaches the client as a JSON problem frame, and is counted as rsocket")
        void chatSend_shouldAnswerATechnicalFailure_asAJsonProblemFrame_andCountIt() {
            when(chatService.sendMessage(any())).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));
            double before = counted("CHT-TEC-004");

            StepVerifier.create(send())
                    .consumeErrorWith(frame -> {
                        JsonNode problem = problemOf(frame);
                        assertThat(problem.get("code").asString()).isEqualTo("CHT-TEC-004");
                        assertThat(problem.get("status").asInt()).isEqualTo(502);
                    })
                    .verify(Duration.ofSeconds(10));
            assertThat(counted("CHT-TEC-004")).isEqualTo(before + 1);
        }

        @Test
        @DisplayName("a failure thrown while the route method runs goes through the @MessageExceptionHandler")
        void chatSend_shouldAnswerASynchronousFailure_asAJsonProblemFrame() {
            when(chatService.sendMessage(any())).thenThrow(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED));

            StepVerifier.create(send())
                    .consumeErrorWith(frame -> assertThat(problemOf(frame).get("code").asString()).isEqualTo("CHT-BUS-011"))
                    .verify(Duration.ofSeconds(10));
        }

        @Test
        @DisplayName("a stream that fails after it was opened ends with a JSON problem frame too")
        void chatStream_shouldEndAFailedStream_withAJsonProblemFrame() {
            when(chatService.streamMessages("user-1")).thenReturn(Flux.never());
            when(liveSessions.attach(eq("user-1"), any()))
                    .thenReturn(Flux.error(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED)));

            StepVerifier.create(requester().route("chat.stream")
                            .metadata(new BearerTokenMetadata(TOKEN), AUTHENTICATION)
                            .metadata(CLIENT_ID, RequestIds.REQUEST_ID_MIME_TYPE)
                            .retrieveFlux(String.class))
                    .consumeErrorWith(frame -> {
                        JsonNode problem = problemOf(frame);
                        assertThat(problem.get("code").asString()).isEqualTo("CHT-BUS-011");
                        assertThat(problem.get("requestId").asString()).isEqualTo(CLIENT_ID);
                    })
                    .verify(Duration.ofSeconds(10));
        }
    }
}
