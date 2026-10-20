package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.DeletedUser;
import com.calendar.chat.domain.models.DeletedUsersPage;
import com.calendar.chat.domain.models.ReconciliationReport;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.DeletedUsersSource;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Runs the erasure again for every wely-users tombstone not erased here yet, or deleted within the recent window. */
public class AccountReconciliationService {

    private static final System.Logger LOG = System.getLogger(AccountReconciliationService.class.getName());
    private static final ReconciliationReport ONE_ERASED = new ReconciliationReport(0, 1, 0);
    private static final ReconciliationReport ONE_FAILED = new ReconciliationReport(0, 0, 1);

    private final DeletedUsersSource source;
    private final DeletedUserMarkers markers;
    private final AccountErasureService erasure;
    private final Clock clock;
    private final Duration recentWindow;
    private final int pageSize;

    public AccountReconciliationService(DeletedUsersSource source, DeletedUserMarkers markers,
                                        AccountErasureService erasure, Clock clock, Duration recentWindow,
                                        int pageSize) {
        this.source = source;
        this.markers = markers;
        this.erasure = erasure;
        this.clock = clock;
        this.recentWindow = recentWindow;
        this.pageSize = pageSize;
    }

    /** One pass over every tombstone; a failed erasure is counted and the pass goes on, a source failure ends it. */
    public Mono<ReconciliationReport> reconcile() {
        return Mono.defer(() -> {
            Instant recentSince = clock.instant().minus(recentWindow);
            // Pages one after the other: the next is fetched once the previous one's erasures are done.
            return reconcilePage(null, recentSince)
                    .expand(page -> page.next() == null ? Mono.empty() : reconcilePage(page.next(), recentSince))
                    .map(PageOutcome::report)
                    .reduce(ReconciliationReport.NONE, ReconciliationReport::plus);
        });
    }

    private Mono<PageOutcome> reconcilePage(String after, Instant recentSince) {
        return source.page(after, pageSize)
                .flatMap(page -> reconcile(page, recentSince)
                        .map(report -> new PageOutcome(report, nextCursor(after, page))));
    }

    // An empty page or a cursor that does not move ends the pass, whatever next says.
    private static String nextCursor(String after, DeletedUsersPage page) {
        return page.items().isEmpty() || Objects.equals(page.next(), after) ? null : page.next();
    }

    private Mono<ReconciliationReport> reconcile(DeletedUsersPage page, Instant recentSince) {
        if (page.items().isEmpty()) {
            return Mono.just(ReconciliationReport.NONE);
        }
        List<String> ids = page.items().stream().map(DeletedUser::userId).toList();
        return markers.erasedAmong(ids)
                .flatMapMany(erased -> Flux.fromIterable(page.items())
                        .filter(user -> isDue(user, erased, recentSince)))
                .concatMap(this::erase)
                .reduce(new ReconciliationReport(page.items().size(), 0, 0), ReconciliationReport::plus);
    }

    // Recent tombstones are erased again: a token still valid after the deletion may have written since.
    private static boolean isDue(DeletedUser user, Set<String> erased, Instant recentSince) {
        return !erased.contains(user.userId())
                || user.deletedAt() == null
                || !user.deletedAt().isBefore(recentSince);
    }

    private Mono<ReconciliationReport> erase(DeletedUser user) {
        return Mono.defer(() -> erasure.erase(user.userId(), user.deletedAt()))
                .thenReturn(ONE_ERASED)
                .onErrorResume(error -> {
                    LOG.log(System.Logger.Level.WARNING, "Reconciliation could not erase account {0}: {1}",
                            user.userId(), reason(error));
                    return Mono.just(ONE_FAILED);
                });
    }

    private static String reason(Throwable error) {
        if (error instanceof TechnicalException technical) {
            return technical.getErrorCode().getCode() + " " + technical.getMessage();
        }
        return error instanceof BusinessException business
                ? business.getErrorCode().getCode() + " " + business.getMessage()
                : error.toString();
    }

    /** A page's counts, and the cursor of the page after it. */
    private record PageOutcome(ReconciliationReport report, String next) {
    }
}
