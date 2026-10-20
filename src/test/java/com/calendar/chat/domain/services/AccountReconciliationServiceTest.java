package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.DeletedUser;
import com.calendar.chat.domain.models.DeletedUsersPage;
import com.calendar.chat.domain.models.ReconciliationReport;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.DeletedUsersSource;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AccountReconciliationServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-16T12:00:00Z");
    private static final Duration RECENT_WINDOW = Duration.ofHours(48);
    private static final int PAGE_SIZE = 500;
    private static final Instant LONG_AGO = NOW.minus(Duration.ofDays(30));
    private static final Instant RECENTLY = NOW.minus(Duration.ofHours(47));

    @Mock
    private DeletedUsersSource source;
    @Mock
    private DeletedUserMarkers markers;
    @Mock
    private AccountErasureService erasure;

    private AccountReconciliationService service;

    @BeforeEach
    void setUp() {
        service = new AccountReconciliationService(source, markers, erasure, Clock.fixed(NOW, ZoneOffset.UTC),
                RECENT_WINDOW, PAGE_SIZE);
    }

    private static DeletedUsersPage page(String next, DeletedUser... items) {
        return new DeletedUsersPage(List.of(items), next);
    }

    private void onePage(DeletedUser... items) {
        when(source.page(null, PAGE_SIZE)).thenReturn(Mono.just(page(null, items)));
    }

    private void erasedHere(String... userIds) {
        when(markers.erasedAmong(anyCollection())).thenReturn(Mono.just(Set.of(userIds)));
    }

    @Test
    @DisplayName("a tombstone with no marker here, or a marker without erasedAt, is erased with its deletedAt")
    void reconcile_shouldErase_whenTheTombstoneIsNotErasedHereYet() {
        onePage(new DeletedUser("alice", LONG_AGO));
        erasedHere();
        when(erasure.erase("alice", LONG_AGO)).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(1, 1, 0))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(erasure).erase("alice", LONG_AGO);
    }

    @Test
    @DisplayName("an account erased here but deleted 48 h ago or less is erased again: its token may still write")
    void reconcile_shouldEraseAgain_whenTheTombstoneIsWithinTheRecentWindow() {
        onePage(new DeletedUser("alice", RECENTLY), new DeletedUser("bob", NOW.minus(RECENT_WINDOW)));
        erasedHere("alice", "bob");
        when(erasure.erase("alice", RECENTLY)).thenReturn(Mono.empty());
        when(erasure.erase("bob", NOW.minus(RECENT_WINDOW))).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(2, 2, 0))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(erasure).erase("alice", RECENTLY);
        verify(erasure).erase("bob", NOW.minus(RECENT_WINDOW));
    }

    @Test
    void reconcile_shouldSkip_whenErasedHereAndOlderThanTheRecentWindow() {
        onePage(new DeletedUser("alice", NOW.minus(Duration.ofHours(49))));
        erasedHere("alice");

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(1, 0, 0))
                .expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(erasure);
    }

    @Test
    @DisplayName("a tombstone without deletedAt cannot be dated, so it is treated as recent")
    void reconcile_shouldEraseAgain_whenTheTombstoneHasNoDeletedAt() {
        onePage(new DeletedUser("alice", null));
        erasedHere("alice");
        when(erasure.erase("alice", null)).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(1, 1, 0))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void reconcile_shouldFollowTheCursor_untilNextIsNull() {
        when(source.page(null, PAGE_SIZE)).thenReturn(Mono.just(page("c1", new DeletedUser("alice", LONG_AGO))));
        when(source.page("c1", PAGE_SIZE)).thenReturn(Mono.just(page("c2", new DeletedUser("bob", LONG_AGO))));
        when(source.page("c2", PAGE_SIZE)).thenReturn(Mono.just(page(null, new DeletedUser("carol", LONG_AGO))));
        erasedHere("alice", "bob", "carol");

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(3, 0, 0))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(source, times(3)).page(any(), anyInt());
    }

    @Test
    @DisplayName("the next page is asked for only once the previous page's erasures are done")
    void reconcile_shouldFetchTheNextPage_afterErasingThePreviousOne() {
        when(source.page(null, PAGE_SIZE)).thenReturn(Mono.just(page("c1", new DeletedUser("alice", LONG_AGO))));
        when(source.page("c1", PAGE_SIZE)).thenReturn(Mono.just(page(null, new DeletedUser("bob", LONG_AGO))));
        erasedHere();
        when(erasure.erase(anyString(), any())).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(2, 2, 0))
                .expectComplete().verify(Duration.ofSeconds(10));

        InOrder order = inOrder(source, erasure);
        order.verify(source).page(null, PAGE_SIZE);
        order.verify(erasure).erase("alice", LONG_AGO);
        order.verify(source).page("c1", PAGE_SIZE);
        order.verify(erasure).erase("bob", LONG_AGO);
    }

    @Test
    void reconcile_shouldReadTheMarkersOncePerPage() {
        onePage(new DeletedUser("alice", LONG_AGO), new DeletedUser("bob", LONG_AGO));
        erasedHere("alice", "bob");

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(2, 0, 0))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(markers).erasedAmong(List.of("alice", "bob"));
    }

    @Test
    @DisplayName("an empty page ends the pass, even if it carries a cursor")
    void reconcile_shouldStop_whenAPageIsEmpty_evenWithANextCursor() {
        when(source.page(null, PAGE_SIZE)).thenReturn(Mono.just(page("c1")));

        StepVerifier.create(service.reconcile())
                .expectNext(ReconciliationReport.NONE)
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(source, never()).page("c1", PAGE_SIZE);
        verifyNoInteractions(markers, erasure);
    }

    @Test
    @DisplayName("a cursor that does not move ends the pass instead of looping on the same page")
    void reconcile_shouldStop_whenTheCursorDoesNotMove() {
        when(source.page(null, PAGE_SIZE)).thenReturn(Mono.just(page("c1", new DeletedUser("alice", LONG_AGO))));
        when(source.page("c1", PAGE_SIZE)).thenReturn(Mono.just(page("c1", new DeletedUser("bob", LONG_AGO))));
        erasedHere("alice", "bob");

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(2, 0, 0))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(source, times(1)).page("c1", PAGE_SIZE);
    }

    @Test
    void reconcile_shouldCountAFailedErasure_andCarryOnWithTheNextTombstone() {
        onePage(new DeletedUser("alice", LONG_AGO), new DeletedUser("bob", LONG_AGO));
        erasedHere();
        when(erasure.erase("alice", LONG_AGO)).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));
        when(erasure.erase("bob", LONG_AGO)).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(2, 1, 1))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("an erasure that throws instead of failing its Mono is counted as failed too")
    void reconcile_shouldCountAFailedErasure_whenTheErasureThrows() {
        onePage(new DeletedUser("alice", LONG_AGO));
        erasedHere();
        when(erasure.erase("alice", LONG_AGO)).thenThrow(new IllegalStateException("boom"));

        StepVerifier.create(service.reconcile())
                .expectNext(new ReconciliationReport(1, 0, 1))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void reconcile_shouldFail_whenTheSourceIsUnavailable() {
        when(source.page(null, PAGE_SIZE)).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.USERS_UNAVAILABLE)));

        StepVerifier.create(service.reconcile())
                .expectErrorMatches(error -> error instanceof TechnicalException technical
                        && technical.getErrorCode() == TechnicalErrorCode.USERS_UNAVAILABLE)
                .verify(Duration.ofSeconds(10));

        verifyNoInteractions(markers, erasure);
    }

    @Test
    @DisplayName("a later page failing fails the pass; the erasures already applied stay, being idempotent")
    void reconcile_shouldFail_whenALaterPageIsUnavailable() {
        when(source.page(null, PAGE_SIZE)).thenReturn(Mono.just(page("c1", new DeletedUser("alice", LONG_AGO))));
        when(source.page("c1", PAGE_SIZE)).thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.USERS_UNAVAILABLE)));
        erasedHere();
        when(erasure.erase("alice", LONG_AGO)).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile()).expectError(TechnicalException.class).verify(Duration.ofSeconds(10));

        verify(erasure).erase("alice", LONG_AGO);
    }

    @Test
    void reconcile_shouldFail_whenTheMarkersCannotBeRead() {
        onePage(new DeletedUser("alice", LONG_AGO));
        when(markers.erasedAmong(anyCollection()))
                .thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(service.reconcile())
                .expectErrorMatches(error -> error instanceof TechnicalException technical
                        && technical.getErrorCode() == TechnicalErrorCode.STORE_UNAVAILABLE)
                .verify(Duration.ofSeconds(10));

        verifyNoInteractions(erasure);
    }

    @Test
    void reconcile_shouldReportNothing_whenThereIsNoTombstone() {
        onePage();

        StepVerifier.create(service.reconcile())
                .expectNext(ReconciliationReport.NONE)
                .expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(markers, erasure);
    }
}
