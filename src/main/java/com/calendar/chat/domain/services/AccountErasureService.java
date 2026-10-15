package com.calendar.chat.domain.services;

import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.PresenceRepository;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;

/** Erases what the chat holds about a deleted account, in idempotent steps that start with its marker. */
public class AccountErasureService {

    private static final System.Logger LOG = System.getLogger(AccountErasureService.class.getName());

    private final DeletedUserMarkers markers;
    private final ChatRepository chatRepository;
    private final PresenceRepository presenceRepository;
    private final CallService callService;
    private final LiveSessions liveSessions;
    private final Clock clock;

    public AccountErasureService(DeletedUserMarkers markers, ChatRepository chatRepository,
                                 PresenceRepository presenceRepository, CallService callService,
                                 LiveSessions liveSessions, Clock clock) {
        this.markers = markers;
        this.chatRepository = chatRepository;
        this.presenceRepository = presenceRepository;
        this.callService = callService;
        this.liveSessions = liveSessions;
        this.clock = clock;
    }

    /** Erases the account; a null {@code deletedAt}, from an older record, falls back on now. */
    public Mono<Void> erase(String userId, Instant deletedAt) {
        Instant at = deletedAt == null ? clock.instant() : deletedAt;
        // Each step deferred: assembled eagerly, a later step would run even when an earlier one failed.
        return markers.markDeleted(userId, at)
                .then(Mono.defer(() -> liveSessions.close(userId)))
                .then(Mono.defer(() -> chatRepository.anonymizeSender(userId)))
                .then(Mono.defer(() -> chatRepository.markDirectConversationsReadOnly(userId)))
                .then(Mono.defer(() -> chatRepository.forgetClosedGroupsOrganizedBy(userId)))
                .then(Mono.defer(() -> callService.removeUser(userId)))
                .then(Mono.defer(() -> presenceRepository.forget(userId)))
                .then(Mono.defer(() -> markers.markErased(userId, clock.instant())))
                .doOnSuccess(done -> LOG.log(System.Logger.Level.INFO, "Account {0} erased", userId));
    }
}
