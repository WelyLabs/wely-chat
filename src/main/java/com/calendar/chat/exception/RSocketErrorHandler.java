package com.calendar.chat.exception;

import io.rsocket.exceptions.ApplicationErrorException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.DestinationPatternsMessageCondition;
import org.springframework.stereotype.Controller;
import org.springframework.util.RouteMatcher;
import org.springframework.web.bind.annotation.ControllerAdvice;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Turns a failure on an RSocket route into an error frame this service controls.
 *
 * <p><b>Frame format — a contract with wely-web and wely-ios.</b> The frame stays
 * {@code APPLICATION_ERROR}; its data, which used to be the bare detail text, is now a JSON
 * problem, the same codes and fields as an HTTP answer minus {@code instance}:
 * <pre>{@code
 * {"type":"https://welylabs.app/problems/cht-bus-011","title":"Account deleted","status":410,
 *  "detail":"This account has been deleted.","code":"CHT-BUS-011",
 *  "timestamp":"2026-10-20T10:15:30.123Z","requestId":"3f2b8c1e-…"}
 * }</pre>
 * A business failure carries its own code and status; a technical one its {@code CHT-TEC-*} code,
 * logged at ERROR with its cause and counted with source {@value GlobalErrorHandler#SOURCE_RSOCKET};
 * anything else is {@value GlobalErrorHandler#UNEXPECTED_FAILURE_CODE}, with a detail that leaks
 * nothing. Clients parse the data as JSON and fall back to plain text for a frame from an older
 * server or from the transport itself.
 *
 * <p>{@code requestId} is the id the client sent in {@link RequestIds#REQUEST_ID_MIME_TYPE}
 * metadata, if it looks like one; otherwise a new one, quoted in the log line so the two meet.
 *
 * <p>A {@code @ControllerAdvice}: Boot registers it with the {@code RSocketMessageHandler}, and with
 * Spring 7 its {@code @MessageExceptionHandler} sees every failure of a route — thrown while the method
 * runs, emitted by the {@code Mono} it returns, or emitted by a {@code Flux} after the stream opened
 * ({@code RSocketErrorHandlerTest} proves all three against a real requester). Authentication
 * failures are refused by the security interceptor before any route, and keep their own frame.
 */
@Slf4j
@ControllerAdvice(annotations = Controller.class)
public class RSocketErrorHandler {

    private final TechnicalErrorCounter counter;
    private final ObjectMapper json;

    public RSocketErrorHandler(TechnicalErrorCounter counter, ObjectMapper json) {
        this.counter = counter;
        this.json = json;
    }

    /** Any failure of an RSocket route, answered as the error frame of {@link #toFrame}. */
    @MessageExceptionHandler
    public Mono<Void> handle(Throwable failure, @Headers Map<String, Object> headers) {
        return Mono.error(toFrame(failure, routeOf(headers), requestIdOf(headers)));
    }

    /** The error frame for a failure on {@code route}: an {@code APPLICATION_ERROR} whose data is the JSON problem. */
    public ApplicationErrorException toFrame(Throwable failure, String route, String requestId) {
        Map<String, Object> problem = switch (failure) {
            case ApplicationErrorException alreadyFramed -> null;
            case BusinessException business -> business(business.getErrorCode(), route, requestId);
            case TechnicalException technical -> technical(technical, route, requestId);
            default -> unexpected(failure, route, requestId);
        };
        return problem == null ? (ApplicationErrorException) failure
                : new ApplicationErrorException(json.writeValueAsString(problem));
    }

    /** The id to quote: the client's, when it sent a valid one, or a new one. */
    public static String requestIdOf(Map<String, Object> headers) {
        return headers.get(RequestIds.REQUEST_ID_ATTRIBUTE) instanceof String sent && RequestIds.isValid(sent)
                ? sent : UUID.randomUUID().toString();
    }

    static String routeOf(Map<String, Object> headers) {
        return headers.get(DestinationPatternsMessageCondition.LOOKUP_DESTINATION_HEADER) instanceof RouteMatcher.Route route
                ? route.value() : "unknown route";
    }

    private Map<String, Object> business(BusinessErrorCode error, String route, String requestId) {
        // Expected outcome, not an incident: logged at WARN, and without a stack trace.
        log.warn("Business failure {} on {} (request {})", error.getCode(), route, requestId);
        return problem(error.getHttpStatus(), error.getCode(), error.getTitle(), error.getDetail(), requestId);
    }

    private Map<String, Object> technical(TechnicalException failure, String route, String requestId) {
        TechnicalErrorCode error = failure.getErrorCode();
        // A dependency is down: ERROR with the cause, because someone has to act on it.
        log.error("Technical failure {} on {} (request {})", error.getCode(), route, requestId, failure);
        counter.count(failure, GlobalErrorHandler.SOURCE_RSOCKET);
        return problem(error.getHttpStatus(), error.getCode(), error.getTitle(), error.getDetail(), requestId);
    }

    private Map<String, Object> unexpected(Throwable failure, String route, String requestId) {
        log.error("Unhandled failure on {} (request {})", route, requestId, failure);
        counter.count(failure, GlobalErrorHandler.SOURCE_RSOCKET);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, GlobalErrorHandler.UNEXPECTED_FAILURE_CODE,
                "Unexpected error", "The request could not be completed.", requestId);
    }

    private static Map<String, Object> problem(HttpStatus status, String code, String title, String detail,
                                               String requestId) {
        Map<String, Object> problem = new LinkedHashMap<>();
        problem.put("type", ProblemDetails.typeOf(code).toString());
        problem.put("title", title);
        problem.put("status", status.value());
        problem.put("detail", detail);
        problem.put(ProblemDetails.CODE_PROPERTY, code);
        problem.put(ProblemDetails.TIMESTAMP_PROPERTY, Instant.now().toString());
        problem.put(ProblemDetails.REQUEST_ID_PROPERTY, requestId);
        return problem;
    }
}
