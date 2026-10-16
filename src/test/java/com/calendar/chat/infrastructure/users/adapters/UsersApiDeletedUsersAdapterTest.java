package com.calendar.chat.infrastructure.users.adapters;

import com.calendar.chat.configuration.UsersApiClientConfig;
import com.calendar.chat.domain.models.DeletedUser;
import com.calendar.chat.domain.models.DeletedUsersPage;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import com.calendar.chat.infrastructure.users.mappers.DeletedUsersMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mapstruct.factory.Mappers;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Requests go to a stub exchange behind the real client configuration: what is checked is what goes out and how answers are read. */
class UsersApiDeletedUsersAdapterTest {

    private static final String URL = "http://dev-wely-users-service:8082";
    private static final String SECRET = "reader-s3cret";
    private static final Instant DELETED_AT = Instant.parse("2026-10-15T14:03:27.123Z");

    private final List<ClientRequest> sent = new ArrayList<>();
    private Supplier<Mono<ClientResponse>> answer;

    @BeforeEach
    void setUp() {
        answer = () -> Mono.just(json(HttpStatus.OK, "{\"items\":[],\"next\":null}"));
    }

    private UsersApiDeletedUsersAdapter adapter(String url, String secret) {
        ExchangeFunction exchange = request -> {
            sent.add(request);
            return answer.get();
        };
        WebClient client = new UsersApiClientConfig().usersWebClient(url, secret).mutate()
                .exchangeFunction(exchange)
                .build();
        return new UsersApiDeletedUsersAdapter(client, Mappers.getMapper(DeletedUsersMapper.class), url, secret);
    }

    private UsersApiDeletedUsersAdapter adapter() {
        return adapter(URL, SECRET);
    }

    private static ClientResponse json(HttpStatus status, String body) {
        return ClientResponse.create(status)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build();
    }

    private static boolean usersUnavailable(Throwable error) {
        return error instanceof ChatException chat && chat.getErrorCode() == ChatErrorCode.USERS_UNAVAILABLE;
    }

    @Test
    void page_shouldSendTheReaderSecretAndTheLimit_withoutAfter_onTheFirstPage() {
        StepVerifier.create(adapter().page(null, 500)).expectNextCount(1).verifyComplete();

        ClientRequest request = sent.getFirst();
        assertThat(request.url().toString()).isEqualTo(URL + "/user-service/internal/deleted-users?limit=500");
        assertThat(request.headers().getFirst(UsersApiClientConfig.SECRET_HEADER)).isEqualTo(SECRET);
    }

    @Test
    @DisplayName("the cursor is sent as a query value, encoded whatever characters it holds")
    void page_shouldEncodeTheCursor_whenFollowingAPage() {
        StepVerifier.create(adapter().page("a+b/c=&d", 500)).expectNextCount(1).verifyComplete();

        assertThat(sent.getFirst().url().getRawQuery()).isEqualTo("limit=500&after=a%2Bb%2Fc%3D%26d");
    }

    @Test
    void page_shouldMapTheItemsAndTheNextCursor() {
        answer = () -> Mono.just(json(HttpStatus.OK, """
                {"items":[{"userId":"89a380b8-0000-4000-8000-000000000001","deletedAt":"2026-10-15T14:03:27.123Z"}],
                 "next":"MjAyNi0xMC0xNVQxNDowMzoyNy4xMjNafDg5YTM4MGI4"}
                """));

        StepVerifier.create(adapter().page(null, 500))
                .expectNext(new DeletedUsersPage(
                        List.of(new DeletedUser("89a380b8-0000-4000-8000-000000000001", DELETED_AT)),
                        "MjAyNi0xMC0xNVQxNDowMzoyNy4xMjNafDg5YTM4MGI4"))
                .verifyComplete();
    }

    @Test
    @DisplayName("an empty items list is the one legitimate empty last page")
    void page_shouldMapAnEmptyItemsList_toAnEmptyLastPage() {
        StepVerifier.create(adapter().page(null, 500))
                .expectNext(new DeletedUsersPage(List.of(), null))
                .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"next\":null}", "{\"content\":[],\"next\":null}"})
    @DisplayName("a body without items fails, so a contract drift is retried in an hour instead of reading as no tombstones")
    void page_shouldFailWithUsersUnavailable_whenItemsIsMissing(String body) {
        answer = () -> Mono.just(json(HttpStatus.OK, body));

        StepVerifier.create(adapter().page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }

    @Test
    @DisplayName("a blank URL fails at once: no request goes out")
    void page_shouldFailWithoutCalling_whenTheUrlIsNotSet() {
        StepVerifier.create(adapter("", SECRET).page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();

        assertThat(sent).isEmpty();
    }

    @Test
    @DisplayName("a blank secret fails at once: wely-users would refuse it anyway")
    void page_shouldFailWithoutCalling_whenTheSecretIsNotSet() {
        StepVerifier.create(adapter(URL, " ").page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();

        assertThat(sent).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 404, 500, 502, 503})
    void page_shouldFailWithUsersUnavailable_whenWelyUsersAnswersAnError(int status) {
        answer = () -> Mono.just(json(HttpStatus.valueOf(status), "{\"code\":\"USR-BUS-013\"}"));

        StepVerifier.create(adapter().page("c1", 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }

    @Test
    void page_shouldFailWithUsersUnavailable_whenTheBodyIsUnreadable() {
        answer = () -> Mono.just(json(HttpStatus.OK, "<html>not json</html>"));

        StepVerifier.create(adapter().page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }

    @Test
    void page_shouldFailWithUsersUnavailable_whenTheBodyIsMissing() {
        answer = () -> Mono.just(ClientResponse.create(HttpStatus.OK).build());

        StepVerifier.create(adapter().page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }

    @Test
    @DisplayName("a tombstone without userId makes the page unreadable rather than erasing a null id")
    void page_shouldFailWithUsersUnavailable_whenATombstoneHasNoUserId() {
        answer = () -> Mono.just(json(HttpStatus.OK, "{\"items\":[{\"deletedAt\":\"2026-10-15T14:03:27Z\"}]}"));

        StepVerifier.create(adapter().page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }

    @Test
    void page_shouldFailWithUsersUnavailable_whenAnIdIsNotAUuid() {
        answer = () -> Mono.just(json(HttpStatus.OK,
                "{\"items\":[{\"userId\":\"camille\",\"deletedAt\":\"2026-10-15T14:03:27Z\"}],\"next\":null}"));

        StepVerifier.create(adapter().page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }

    @Test
    void page_shouldFailWithUsersUnavailable_whenWelyUsersCannotBeReached() {
        answer = () -> Mono.error(new IOException("Connection refused"));

        StepVerifier.create(adapter().page(null, 500))
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }

    @Test
    void page_shouldFailWithUsersUnavailable_whenWelyUsersTimesOut() {
        answer = Mono::never;
        UsersApiDeletedUsersAdapter adapter = adapter();

        StepVerifier.withVirtualTime(() -> adapter.page(null, 500))
                .expectSubscription()
                .thenAwait(UsersApiDeletedUsersAdapter.TIMEOUT)
                .expectErrorMatches(UsersApiDeletedUsersAdapterTest::usersUnavailable)
                .verify();
    }
}
