package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class StoreErrorsTest {

    @Test
    void unavailable_shouldTranslateAMongoFailureIntoStoreUnavailable_withoutItsMessage() {
        var translated = StoreErrors.unavailable(new DataAccessResourceFailureException("mongodb://admin:secret@host"));

        assertThat(translated.getErrorCode()).isEqualTo(TechnicalErrorCode.STORE_UNAVAILABLE);
        assertThat(translated.getMessage()).doesNotContain("secret");
    }

    @Test
    void unavailable_shouldKeepTheMongoFailureAsTheCause() {
        var failure = new DataAccessResourceFailureException("connection refused");

        assertThat(StoreErrors.unavailable(failure).getCause()).isSameAs(failure);
    }

    private static String stackTraceOf(Throwable failure) {
        java.io.StringWriter trace = new java.io.StringWriter();
        failure.printStackTrace(new java.io.PrintWriter(trace));
        return trace.toString();
    }

    @Test
    @DisplayName("a real driver failure, kept as the cause, never prints the connection string's password")
    void unavailable_shouldNotLogTheConnectionStringPassword_ofARealDriverFailure() {
        try (MongoClient client = MongoClients.create("mongodb://wely:s3cr3t@unreachable.invalid:27017/chat"
                + "?serverSelectionTimeoutMS=1000&connectTimeoutMS=500")) {
            ReactiveMongoTemplate template = new ReactiveMongoTemplate(client, "chat");

            Throwable failure = catchThrowable(() -> template.executeCommand("{ping: 1}")
                    .onErrorMap(DataAccessException.class, StoreErrors::unavailable)
                    .block(Duration.ofSeconds(10)));

            assertThat(failure).isInstanceOf(TechnicalException.class);
            assertThat(stackTraceOf(failure)).contains("unreachable.invalid").doesNotContain("s3cr3t");
        }
    }
}
