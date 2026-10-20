package com.calendar.chat.exception;

import org.springframework.core.ResolvableType;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.codec.HttpMessageWriter;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Writes a problem straight to the response, for the places no controller advice reaches:
 * security entry points and the last-resort {@code WebExceptionHandler}.
 *
 * <p>Goes through the same codecs as the controllers, so the body is serialized exactly as an
 * advice's {@link ProblemDetail} would be — {@code code} and {@code requestId} at the top level.
 */
@Component
public class ProblemResponseWriter {

    private static final ResolvableType PROBLEM_TYPE = ResolvableType.forClass(ProblemDetail.class);

    private final HttpMessageWriter<ProblemDetail> writer;

    @SuppressWarnings("unchecked")
    public ProblemResponseWriter(ServerCodecConfigurer codecs) {
        this.writer = (HttpMessageWriter<ProblemDetail>) codecs.getWriters().stream()
                .filter(candidate -> candidate.canWrite(PROBLEM_TYPE, MediaType.APPLICATION_PROBLEM_JSON))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No codec writes application/problem+json"));
    }

    public Mono<Void> write(ServerWebExchange exchange, ProblemDetail problem) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(HttpStatusCode.valueOf(problem.getStatus()));
        response.getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        return writer.write(Mono.just(problem), PROBLEM_TYPE, MediaType.APPLICATION_PROBLEM_JSON, response, Map.of());
    }
}
