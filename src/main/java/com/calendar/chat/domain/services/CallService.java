package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallEventType;
import com.calendar.chat.domain.models.CallJoin;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.domain.models.CallSubscription;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.LayerChoice;
import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.RemoteTrack;
import com.calendar.chat.domain.models.SessionDescription;
import com.calendar.chat.domain.models.TrackRequest;
import com.calendar.chat.domain.ports.CallBroadcaster;
import com.calendar.chat.domain.ports.CallNoticeFormatter;
import com.calendar.chat.domain.ports.CallRepository;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.IceServerProvider;
import com.calendar.chat.domain.ports.MediaRelay;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Audio and video calls in a conversation. This service decides who may do what; the media
 * itself goes from browser to browser through the {@link MediaRelay}, never through here.
 *
 * <p>Every operation takes the caller's id from their token. A session id or a track is never
 * taken from the client as is: it is looked up in the stored call, so nobody can publish on
 * someone else's session or receive a track from another call.
 */
public class CallService {

    private static final System.Logger LOG = System.getLogger(CallService.class.getName());

    /** How often a browser in a call sends a heartbeat. */
    public static final Duration HEARTBEAT = Duration.ofSeconds(15);
    /** Three missed heartbeats: the browser crashed, lost its network or was closed. */
    static final Duration SILENCE_TIMEOUT = HEARTBEAT.multipliedBy(3);
    /** Nobody answered in time. */
    static final Duration RING_TIMEOUT = Duration.ofSeconds(30);
    /** A guard on the relay's free allowance: a call forgotten open does not run all night. */
    static final Duration MAX_DURATION = Duration.ofHours(3);

    private final CallRepository callRepository;
    private final ChatRepository chatRepository;
    private final MediaRelay mediaRelay;
    private final IceServerProvider iceServerProvider;
    private final CallBroadcaster callBroadcaster;
    private final ChatService chatService;
    private final CallNoticeFormatter noticeFormatter;
    private final AccountGuard guard;
    private final int maxParticipants;
    private final Clock clock;

    public CallService(CallRepository callRepository, ChatRepository chatRepository, MediaRelay mediaRelay,
                       IceServerProvider iceServerProvider, CallBroadcaster callBroadcaster, ChatService chatService,
                       CallNoticeFormatter noticeFormatter, AccountGuard guard, int maxParticipants, Clock clock) {
        this.callRepository = callRepository;
        this.chatRepository = chatRepository;
        this.mediaRelay = mediaRelay;
        this.iceServerProvider = iceServerProvider;
        this.callBroadcaster = callBroadcaster;
        this.chatService = chatService;
        this.noticeFormatter = noticeFormatter;
        this.guard = guard;
        this.maxParticipants = maxParticipants;
        this.clock = clock;
    }

    /** Rings the other members of a conversation the caller may write to, or returns its live call. */
    public Mono<Call> start(String userId, String conversationId, CallMedia media) {
        return guard.requireActive(userId)
                .then(Mono.defer(() -> chatRepository.findById(conversationId, userId)))
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CONVERSATION_NOT_FOUND)))
                .flatMap(conversation -> conversation.isReadOnly()
                        ? Mono.<ConversationDetail>error(new ChatException(ChatErrorCode.CONVERSATION_READ_ONLY))
                        : Mono.just(conversation))
                .flatMap(conversation -> callRepository.findLive(conversationId)
                        .switchIfEmpty(Mono.defer(() -> callRepository
                                .createOrGetLive(Call.ringing(conversationId, userId, media, clock.instant(),
                                        conversation.getParticipantIds()))
                                .flatMap(call -> startedBy(call, userId)
                                        ? notify(call, CallEventType.RINGING, userId, call.memberIdsExcept(userId))
                                        : Mono.just(call)))));
    }

    /** The live call of a conversation the caller belongs to; empty when there is none. */
    public Mono<Call> liveCall(String userId, String conversationId) {
        return chatRepository.findById(conversationId, userId)
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CONVERSATION_NOT_FOUND)))
                .flatMap(conversation -> callRepository.findLive(conversationId));
    }

    /** Puts the caller in a live call with a fresh relay session, replacing any previous one. */
    public Mono<CallJoin> join(String userId, String callId) {
        return guard.requireActive(userId)
                .then(Mono.defer(() -> liveCallOfMember(userId, callId)))
                .flatMap(call -> {
                    boolean alreadyIn = call.participant(userId).isPresent();
                    if (!alreadyIn && call.participants().size() >= maxParticipants) {
                        return Mono.error(new ChatException(ChatErrorCode.CALL_FULL));
                    }
                    return mediaRelay.newSession()
                            .flatMap(sessionId -> callRepository
                                    .addParticipant(callId, newParticipant(userId, sessionId), maxParticipants)
                                    .switchIfEmpty(Mono.defer(() -> whyNotJoined(userId, callId)))
                                    .flatMap(this::activateOnceAnswered)
                                    .flatMap(joined -> notify(joined, CallEventType.PARTICIPANT_JOINED, userId,
                                            joined.participantIdsExcept(userId)))
                                    .flatMap(joined -> iceServerProvider.iceServers()
                                            .map(iceServers -> new CallJoin(joined, sessionId, iceServers))));
                });
    }

    /** Publishes the caller's tracks on their own session; emits the relay's answer. */
    public Mono<SessionDescription> publish(String userId, String callId, SessionDescription offer,
                                            List<LocalTrack> tracks) {
        return participantIn(userId, callId)
                .flatMap(entry -> mediaRelay.publish(entry.participant().sessionId(), offer, tracks)
                        .flatMap(answer -> callRepository
                                .addTracks(callId, userId, tracks.stream().map(LocalTrack::trackName).toList())
                                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CALL_NOT_FOUND)))
                                .flatMap(call -> notify(call, CallEventType.TRACKS_PUBLISHED, userId,
                                        call.participantIdsExcept(userId)))
                                .thenReturn(answer)));
    }

    /** Subscribes the caller to tracks published by other participants of the same call. */
    public Mono<CallSubscription> subscribe(String userId, String callId, List<TrackRequest> requests) {
        return participantIn(userId, callId)
                .flatMap(entry -> {
                    List<RemoteTrack> tracks = requests.stream()
                            .map(request -> new RemoteTrack(publishedSession(entry.call(), request.userId(),
                                    request.trackName()), request.trackName(), request.layer(), null))
                            .toList();
                    Map<String, String> userBySession = entry.call().participants().stream()
                            .collect(Collectors.toMap(CallParticipant::sessionId, CallParticipant::userId));
                    return mediaRelay.subscribe(entry.participant().sessionId(), tracks)
                            .map(subscription -> new CallSubscription(subscription.offer(),
                                    subscription.requiresRenegotiation(),
                                    subscription.tracks().stream()
                                            .map(track -> new CallSubscription.ReceivedTrack(track.mid(),
                                                    userBySession.get(track.sessionId()), track.trackName()))
                                            .toList()));
                });
    }

    /** Completes a renegotiation the relay asked for, on the caller's own session. */
    public Mono<Void> renegotiate(String userId, String callId, SessionDescription answer) {
        return participantIn(userId, callId)
                .flatMap(entry -> mediaRelay.renegotiate(entry.participant().sessionId(), answer));
    }

    /** Switches cameras the caller receives to other simulcast layers. */
    public Mono<Void> chooseLayers(String userId, String callId, List<LayerChoice> choices) {
        return participantIn(userId, callId)
                .flatMap(entry -> mediaRelay.updateLayers(entry.participant().sessionId(), choices.stream()
                        .map(choice -> new RemoteTrack(publishedSession(entry.call(), choice.userId(),
                                choice.trackName()), choice.trackName(), choice.layer(), choice.mid()))
                        .toList()));
    }

    /** Stops receiving the tracks on these transceivers, such as those of someone who left. */
    public Mono<Void> unsubscribe(String userId, String callId, List<String> mids) {
        return participantIn(userId, callId)
                .flatMap(entry -> mediaRelay.closeTracks(entry.participant().sessionId(), mids));
    }

    /** Keeps the caller in the call; one who stops sending heartbeats is dropped. */
    public Mono<Void> heartbeat(String userId, String callId) {
        return callRepository.touch(callId, userId, clock.instant())
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CALL_NOT_FOUND)))
                .then();
    }

    /**
     * Takes the caller out of the call. The last one out ends it: ENDED once it was answered,
     * MISSED when the caller hangs up before anyone did. Leaving twice is not an error.
     */
    public Mono<Void> leave(String userId, String callId) {
        return callRepository.removeParticipant(callId, userId)
                .flatMap(call -> call.participants().isEmpty()
                        ? end(call, call.status() == CallStatus.ACTIVE ? CallStatus.ENDED : CallStatus.MISSED, userId)
                        : notify(call, CallEventType.PARTICIPANT_LEFT, userId, call.participantIds()))
                .then();
    }

    /** A member refuses a ringing call: a direct call ends, a group call only tells those in it. */
    public Mono<Void> decline(String userId, String callId) {
        return guard.requireActive(userId)
                .then(Mono.defer(() -> liveCallOfMember(userId, callId)))
                .flatMap(call -> {
                    if (call.status() == CallStatus.RINGING && call.memberIds().size() == 2) {
                        return end(call, CallStatus.DECLINED, userId);
                    }
                    return notify(call, CallEventType.DECLINED, userId, call.participantIdsExcept(userId));
                })
                .then();
    }

    /** Takes a deleted account out of every call, never ending one MISSED. */
    public Mono<Void> removeUser(String userId) {
        return callRepository.findLiveInvolving(userId)
                .concatMap(call -> call.participant(userId)
                        // Withdrawn before its session closes, so subscribe or renegotiate cannot reopen tracks.
                        .map(participant -> withdraw(userId, call.id()).then(Mono.defer(() -> cutOff(participant))))
                        .orElseGet(() -> ringsInVain(call, userId)
                                ? end(call, CallStatus.ENDED, userId).then()
                                : Mono.empty()))
                .then(Mono.defer(() -> callRepository.removeFromEndedCalls(userId)));
    }

    /** A ringing call nobody can answer any more: the deleted account rang, or was the only one rung. */
    private static boolean ringsInVain(Call call, String userId) {
        return call.status() == CallStatus.RINGING
                && (call.startedBy().equals(userId) || call.memberIds().size() == 2);
    }

    /** Best-effort: a relay that cannot be reached still lets the participant be taken out. */
    private Mono<Void> cutOff(CallParticipant participant) {
        return mediaRelay.closeSession(participant.sessionId())
                .onErrorResume(error -> {
                    LOG.log(System.Logger.Level.WARNING, "Relay session of a deleted account not closed: {0}",
                            error.toString());
                    return Mono.empty();
                });
    }

    /** Like {@link #leave}, but the last one out ends the call ENDED, never MISSED. */
    private Mono<Void> withdraw(String userId, String callId) {
        return callRepository.removeParticipant(callId, userId)
                .flatMap(call -> call.participants().isEmpty()
                        ? end(call, CallStatus.ENDED, userId)
                        : notify(call, CallEventType.PARTICIPANT_LEFT, userId, call.participantIds()))
                .then();
    }

    /** Call events addressed to this user, from any instance. */
    public Flux<CallEvent> streamEvents(String userId) {
        return callBroadcaster.subscribe(userId);
    }

    /**
     * Cleans up what nobody closed, run periodically by every instance: unanswered calls stop
     * ringing, silent participants are taken out, and calls past {@link #MAX_DURATION} end. Each
     * step is safe to run twice at once: the writes are conditional, so only one instance acts.
     */
    public Mono<Void> expire() {
        Instant now = clock.instant();
        Mono<Void> unanswered = callRepository.findRingingStartedBefore(now.minus(RING_TIMEOUT))
                .concatMap(call -> end(call, CallStatus.MISSED, call.startedBy()))
                .then();
        Mono<Void> silent = callRepository.findLiveWithParticipantSeenBefore(now.minus(SILENCE_TIMEOUT))
                .concatMap(call -> Flux.fromIterable(call.participants())
                        .filter(participant -> participant.lastSeenAt().isBefore(now.minus(SILENCE_TIMEOUT)))
                        .concatMap(participant -> leave(participant.userId(), call.id())))
                .then();
        Mono<Void> overdue = callRepository.findLiveStartedBefore(now.minus(MAX_DURATION))
                .concatMap(call -> end(call, CallStatus.ENDED, call.startedBy()))
                .then();
        return unanswered.then(silent).then(overdue);
    }

    /**
     * Ends a live call, tells every member and leaves a line in the conversation. Does nothing
     * if it had already ended, so a call ends once even when two instances race to end it.
     */
    Mono<Call> end(Call call, CallStatus status, String actorId) {
        return callRepository.end(call.id(), status, clock.instant())
                .flatMap(ended -> notify(ended, CallEventType.ENDED, actorId, ended.memberIds()))
                .flatMap(ended -> chatService
                        .postConversationNotice(ended.conversationId(), "call:" + ended.id(),
                                noticeFormatter.format(ended))
                        .onErrorResume(error -> {
                            LOG.log(System.Logger.Level.WARNING, "No notice for call {0}: {1}",
                                    ended.id(), error.toString());
                            return Mono.empty();
                        })
                        .thenReturn(ended));
    }

    private Mono<Call> liveCallOfMember(String userId, String callId) {
        return callRepository.findById(callId, userId)
                .filter(Call::isLive)
                // Membership is checked again on the conversation: someone who left an event since
                // the call started is no longer let in.
                .filterWhen(call -> chatRepository.findById(call.conversationId(), userId).hasElement())
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CALL_NOT_FOUND)));
    }

    private Mono<Participation> participantIn(String userId, String callId) {
        return callRepository.findById(callId, userId)
                .filter(Call::isLive)
                .flatMap(call -> Mono.justOrEmpty(call.participant(userId))
                        .map(participant -> new Participation(call, participant)))
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CALL_NOT_FOUND)));
    }

    /** The relay session a track is published on, if it really is published in this call. */
    private static String publishedSession(Call call, String publisherId, String trackName) {
        return call.participant(publisherId)
                .filter(publisher -> publisher.publishes(trackName))
                .map(CallParticipant::sessionId)
                .orElseThrow(() -> new ChatException(ChatErrorCode.CALL_TRACK_DENIED));
    }

    /** addParticipant found no room: the call ended meanwhile, or filled up. */
    private Mono<Call> whyNotJoined(String userId, String callId) {
        return callRepository.findById(callId, userId)
                .filter(Call::isLive)
                .flatMap(call -> Mono.<Call>error(new ChatException(ChatErrorCode.CALL_FULL)))
                .switchIfEmpty(Mono.error(new ChatException(ChatErrorCode.CALL_NOT_FOUND)));
    }

    private Mono<Call> activateOnceAnswered(Call call) {
        return call.status() == CallStatus.RINGING && call.participants().size() >= 2
                ? callRepository.markActive(call.id())
                : Mono.just(call);
    }

    private CallParticipant newParticipant(String userId, String sessionId) {
        return new CallParticipant(userId, sessionId, clock.instant(), clock.instant(), List.of());
    }

    private static boolean startedBy(Call call, String userId) {
        return call.startedBy().equals(userId) && call.participants().isEmpty();
    }

    /** Pushes an event to each recipient. A failed push is skipped: the call goes on regardless. */
    private Mono<Call> notify(Call call, CallEventType type, String actorId, List<String> recipientIds) {
        return Flux.fromIterable(recipientIds)
                .concatMap(recipientId -> callBroadcaster.broadcast(new CallEvent(type, recipientId, actorId, call))
                        .onErrorResume(error -> {
                            LOG.log(System.Logger.Level.WARNING, "Call event {0} not pushed to {1}: {2}",
                                    type, recipientId, error.toString());
                            return Mono.empty();
                        }))
                .then(Mono.just(call));
    }

    private record Participation(Call call, CallParticipant participant) {}
}
