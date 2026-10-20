package com.calendar.chat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;
import com.calendar.chat.configuration.UsersApiClientConfig;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Covers {@code logback-spring.xml}: a URL password never reaches the console, whether it sits in
 * the message or in a stack trace.
 *
 * <p>The leak is real, not hypothetical: when a dependency answers an error, WebClient puts the
 * request URL, userinfo included, in the exception message ({@code 401 Unauthorized from GET
 * http://user:password@…}) and in its checkpoint. The first assertion of each test proves the
 * secret is in the raw text before the layout masks it.
 */
class LogbackSpringXmlTest {

    private static final String SECRET = "s3cr3t";

    private OutputStreamAppender<ILoggingEvent> console;
    private Logger logger;
    private HttpServer server;

    @BeforeEach
    void configure() throws Exception {
        LoggerContext context = new LoggerContext();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(getClass().getResource("/logback-spring.xml"));
        logger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        @SuppressWarnings("unchecked")
        OutputStreamAppender<ILoggingEvent> appender = (OutputStreamAppender<ILoggingEvent>) logger.getAppender("CONSOLE");
        console = appender;

        // A real wely-users stand-in that refuses every call: the error-answer case, where the URL leaks.
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private String format(String message, Throwable error) {
        LoggingEvent event = new LoggingEvent(Logger.class.getName(), logger, Level.ERROR, message, error, null);
        return new String(console.getEncoder().encode(event), StandardCharsets.UTF_8);
    }

    private static String trace(Throwable error) {
        StringWriter out = new StringWriter();
        error.printStackTrace(new PrintWriter(out));
        return out.toString();
    }

    /** What the ERROR log of GlobalErrorHandler receives when wely-users' URL carries a password. */
    private TechnicalException usersRefusalThroughAUrlWithAPassword() {
        Throwable failure = catchThrowable(() -> new UsersApiClientConfig()
                .usersWebClient("http://wely:" + SECRET + "@127.0.0.1:" + server.getAddress().getPort(), "header-secret")
                .get().uri("/user-service/internal/deleted-users")
                .retrieve().toBodilessEntity()
                .block(Duration.ofSeconds(10)));
        assertThat(failure).isInstanceOf(WebClientResponseException.Unauthorized.class);
        return new TechnicalException(TechnicalErrorCode.USERS_UNAVAILABLE, failure);
    }

    @Test
    void consolePattern_shouldMaskAUrlPassword_inTheStackTraceOfAnErrorAnswer() {
        TechnicalException failure = usersRefusalThroughAUrlWithAPassword();
        assertThat(trace(failure)).contains(SECRET);

        String line = format("Technical failure CHT-TEC-003 on /x (request 1)", failure);

        assertThat(line)
                .doesNotContain(SECRET)
                .contains("401 Unauthorized from GET http://wely:****@127.0.0.1")
                .contains("WebClientResponseException");
    }

    @Test
    void consolePattern_shouldPrintTheStackTraceOnce() {
        String line = format("Technical failure", usersRefusalThroughAUrlWithAPassword());

        // Without %nopex, logback appends its own, unmasked copy of the trace.
        assertThat(line.split("Caused by: org.springframework.web.reactive.function.client.WebClientResponseException", -1))
                .hasSize(2);
    }

    @Test
    void consolePattern_shouldMaskAUrlPassword_inTheMessage() {
        String line = format("Deleted accounts not listed from wely-users: 401 Unauthorized from GET http://wely:" + SECRET
                + "@wely-users:8082/user-service/internal/deleted-users", null);

        assertThat(line).doesNotContain(SECRET)
                .contains("http://wely:****@wely-users:8082/user-service/internal/deleted-users");
    }

    @Test
    void consolePattern_shouldLeaveAUrlWithoutPasswordAlone() {
        String line = format("Request to GET http://127.0.0.1:8082/user-service/internal/deleted-users", null);

        assertThat(line).contains("http://127.0.0.1:8082/user-service/internal/deleted-users");
    }
}
