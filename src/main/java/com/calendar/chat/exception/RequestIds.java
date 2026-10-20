package com.calendar.chat.exception;

import org.springframework.util.MimeType;
import org.springframework.web.server.ServerWebExchange;

import java.util.regex.Pattern;

/**
 * The request id of an exchange: the one the gateway sent in {@value #REQUEST_ID_HEADER}, kept by
 * {@code RequestIdWebFilter}, or WebFlux's own when there was none. Every {@code requestId} in a
 * problem and every handler log line reads it here, so the gateway, this service and the client
 * all quote the same id.
 */
public final class RequestIds {

    /** The header the gateway sets on every forwarded request, and this service on every response. */
    public static final String REQUEST_ID_HEADER = "X-Request-Id";

    /** Exchange attribute, and Reactor Context key, holding the id. */
    public static final String REQUEST_ID_ATTRIBUTE = "wely.requestId";

    /** RSocket metadata a client may send its own id in; extracted under {@value #REQUEST_ID_ATTRIBUTE}. */
    public static final MimeType REQUEST_ID_MIME_TYPE = MimeType.valueOf("message/x.wely.request-id");

    /** Letters, digits and dashes, 8 to 64: nothing that could forge a log line or bloat a header. */
    private static final Pattern VALID_ID = Pattern.compile("^[A-Za-z0-9-]{8,64}$");

    private RequestIds() {
    }

    /** Whether an id received from outside can be adopted as is. */
    public static boolean isValid(String id) {
        return id != null && VALID_ID.matcher(id).matches();
    }

    public static String of(ServerWebExchange exchange) {
        return exchange.getAttribute(REQUEST_ID_ATTRIBUTE) instanceof String id ? id : exchange.getRequest().getId();
    }
}
