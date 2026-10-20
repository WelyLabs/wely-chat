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
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.IceServer;
import com.calendar.chat.domain.models.LayerChoice;
import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.RelaySubscription;
import com.calendar.chat.domain.models.RemoteTrack;
import com.calendar.chat.domain.models.SessionDescription;
import com.calendar.chat.domain.models.TrackRequest;
import com.calendar.chat.domain.models.VideoLayer;
import com.calendar.chat.domain.ports.CallBroadcaster;
import com.calendar.chat.domain.ports.CallNoticeFormatter;
import com.calendar.chat.domain.ports.CallRepository;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.IceServerProvider;
import com.calendar.chat.domain.ports.MediaRelay;
import com.calendar.chat.domain.ports.RecordingChatActivity;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CallServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");
    private static final int MAX_PARTICIPANTS = 3;
    private static final List<IceServer> ICE = List.of(new IceServer(List.of("stun:stun.example:3478"), null, null));

    @Mock
    private CallRepository callRepository;
    @Mock
    private ChatRepository chatRepository;
    @Mock
    private MediaRelay mediaRelay;
    @Mock
    private IceServerProvider iceServerProvider;
    @Mock
    private CallBroadcaster callBroadcaster;
    @Mock
    private ChatService chatService;
    @Mock
    private CallNoticeFormatter noticeFormatter;
    @Mock
    private DeletedUserMarkers markers;

    private CallService service;
    private final RecordingChatActivity activity = new RecordingChatActivity();

    @BeforeEach
    void setUp() {
        service = new CallService(callRepository, chatRepository, mediaRelay, iceServerProvider, callBroadcaster,
                chatService, noticeFormatter, new AccountGuard(markers), MAX_PARTICIPANTS, activity,
                Clock.fixed(NOW, ZoneOffset.UTC));
        lenient().when(markers.isDeleted(anyString())).thenReturn(Mono.just(false));
        lenient().when(callBroadcaster.broadcast(any())).thenReturn(Mono.empty());
        lenient().when(noticeFormatter.format(any())).thenReturn("📞 Call");
        lenient().when(chatService.postConversationNotice(anyString(), anyString(), anyString()))
                .thenReturn(Mono.just(Message.system("notice", "📞 Call", NOW)));
        lenient().when(chatService.requireFriendsInDirect(any(), anyString())).thenReturn(Mono.empty());
    }

    private static ConversationDetail conversation(String... members) {
        return new ConversationDetail("conv-1", List.of(members), ConversationType.EVENT, List.of(), 0,
                "Apéro", "event-1");
    }

    private static CallParticipant in(String userId, String... tracks) {
        return new CallParticipant(userId, "session-" + userId, NOW, NOW, List.of(tracks));
    }

    private static Call call(CallStatus status, List<String> members, CallParticipant... participants) {
        return new Call("call-1", "conv-1", "alice", "Alice", CallMedia.VIDEO, status, NOW, null, members,
                List.of(participants));
    }

    private void memberOfConversation(String userId) {
        when(chatRepository.findById("conv-1", userId)).thenReturn(Mono.just(conversation("alice", "bob", "carol")));
    }

    private List<CallEvent> broadcastEvents(int count) {
        ArgumentCaptor<CallEvent> events = ArgumentCaptor.forClass(CallEvent.class);
        verify(callBroadcaster, times(count)).broadcast(events.capture());
        return events.getAllValues();
    }

    // --- start ---------------------------------------------------------------------------

    @Test
    void start_shouldCreateARingingCall_andRingEveryOtherMember() {
        memberOfConversation("alice");
        when(callRepository.findLive("conv-1")).thenReturn(Mono.empty());
        Call created = call(CallStatus.RINGING, List.of("alice", "bob", "carol"));
        when(callRepository.createOrGetLive(any())).thenReturn(Mono.just(created));

        StepVerifier.create(service.start("alice", "Alice", "conv-1", CallMedia.VIDEO)).expectNext(created).expectComplete().verify(Duration.ofSeconds(10));

        ArgumentCaptor<Call> saved = ArgumentCaptor.forClass(Call.class);
        verify(callRepository).createOrGetLive(saved.capture());
        assertThat(saved.getValue().memberIds()).containsExactly("alice", "bob", "carol");
        assertThat(saved.getValue().startedAt()).isEqualTo(NOW);
        assertThat(saved.getValue().startedByName()).isEqualTo("Alice");
        assertThat(broadcastEvents(2))
                .extracting(CallEvent::type, CallEvent::recipientId, CallEvent::actorId)
                .containsExactly(
                        tuple(CallEventType.RINGING, "bob", "alice"),
                        tuple(CallEventType.RINGING, "carol", "alice"));

        assertThat(activity.callsStarted).isEqualTo(1);
    }

    @Test
    void start_shouldStillRing_whenTheCallerHasNoName() {
        memberOfConversation("alice");
        when(callRepository.findLive("conv-1")).thenReturn(Mono.empty());
        Call created = call(CallStatus.RINGING, List.of("alice", "bob"));
        when(callRepository.createOrGetLive(any())).thenReturn(Mono.just(created));

        StepVerifier.create(service.start("alice", null, "conv-1", CallMedia.AUDIO)).expectNext(created)
                .expectComplete().verify(Duration.ofSeconds(10));

        ArgumentCaptor<Call> saved = ArgumentCaptor.forClass(Call.class);
        verify(callRepository).createOrGetLive(saved.capture());
        assertThat(saved.getValue().startedByName()).isNull();
        assertThat(broadcastEvents(1)).extracting(CallEvent::recipientId).containsExactly("bob");
    }

    @Test
    void start_shouldReturnTheLiveCall_whenTheConversationAlreadyHasOne() {
        memberOfConversation("bob");
        Call live = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), in("alice"), in("carol"));
        when(callRepository.findLive("conv-1")).thenReturn(Mono.just(live));

        StepVerifier.create(service.start("bob", "Bob", "conv-1", CallMedia.AUDIO)).expectNext(live).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository, never()).createOrGetLive(any());
        verifyNoInteractions(callBroadcaster);

        assertThat(activity.callsStarted).isZero();
    }

    @Test
    void start_shouldNotRingAgain_whenAnotherMemberStartedTheSameCallAtTheSameTime() {
        memberOfConversation("bob");
        when(callRepository.findLive("conv-1")).thenReturn(Mono.empty());
        Call startedByAlice = call(CallStatus.RINGING, List.of("alice", "bob", "carol"));
        when(callRepository.createOrGetLive(any())).thenReturn(Mono.just(startedByAlice));

        StepVerifier.create(service.start("bob", "Bob", "conv-1", CallMedia.AUDIO)).expectNext(startedByAlice)
                .expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(callBroadcaster);

        assertThat(activity.callsStarted).isZero();
    }

    @Test
    void start_shouldRefuse_whenTheCallerIsNotInTheConversation() {
        when(chatRepository.findById("conv-1", "mallory")).thenReturn(Mono.empty());

        StepVerifier.create(service.start("mallory", "Mallory", "conv-1", CallMedia.AUDIO))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CONVERSATION_NOT_FOUND)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(callRepository);

        assertThat(activity.callsStarted).isZero();
    }

    @Test
    void start_shouldRefuseWithAccountDeleted_whenTheCallerIsDeleted() {
        when(markers.isDeleted("alice")).thenReturn(Mono.just(true));

        StepVerifier.create(service.start("alice", "Alice", "conv-1", CallMedia.AUDIO))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.ACCOUNT_DELETED)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(chatRepository, callRepository, callBroadcaster);
    }

    @Test
    void start_shouldRefuseWithReadOnly_inAReadOnlyConversation() {
        ConversationDetail readOnly = new ConversationDetail("conv-1", List.of("alice", "bob"), ConversationType.DIRECT,
                List.of(), 0, null, null, null, true);
        when(chatRepository.findById("conv-1", "alice")).thenReturn(Mono.just(readOnly));

        StepVerifier.create(service.start("alice", "Alice", "conv-1", CallMedia.AUDIO))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CONVERSATION_READ_ONLY)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(callRepository, callBroadcaster);

        assertThat(activity.callsStarted).isZero();
    }

    @Test
    @DisplayName("nobody rings a private conversation with someone who is not their friend")
    void start_shouldRefuseWithFriendshipRequired_andRingNobody_whenTheyAreNotFriends() {
        ConversationDetail direct = new ConversationDetail("conv-1", List.of("alice", "bob"), ConversationType.DIRECT,
                List.of(), 0, null, null);
        when(chatRepository.findById("conv-1", "alice")).thenReturn(Mono.just(direct));
        when(chatService.requireFriendsInDirect(direct, "alice"))
                .thenReturn(Mono.error(new BusinessException(BusinessErrorCode.FRIENDSHIP_REQUIRED)));

        StepVerifier.create(service.start("alice", "Alice", "conv-1", CallMedia.AUDIO))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.FRIENDSHIP_REQUIRED)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(callRepository, callBroadcaster);

        assertThat(activity.callsStarted).isZero();
    }

    // --- liveCall ------------------------------------------------------------------------

    @Test
    void liveCall_shouldGiveTheConversationsLiveCall_toAMember() {
        memberOfConversation("bob");
        Call live = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"));
        when(callRepository.findLive("conv-1")).thenReturn(Mono.just(live));

        StepVerifier.create(service.liveCall("bob", "conv-1")).expectNext(live).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void liveCall_shouldRefuse_someoneOutsideTheConversation() {
        when(chatRepository.findById("conv-1", "mallory")).thenReturn(Mono.empty());

        StepVerifier.create(service.liveCall("mallory", "conv-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CONVERSATION_NOT_FOUND)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(callRepository);
    }

    // --- join ----------------------------------------------------------------------------

    @Test
    void join_shouldOpenASession_answerTheCall_andTellThoseAlreadyIn() {
        Call ringing = call(CallStatus.RINGING, List.of("alice", "bob", "carol"), in("alice"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(ringing));
        memberOfConversation("bob");
        when(mediaRelay.newSession()).thenReturn(Mono.just("session-bob"));
        Call joined = call(CallStatus.RINGING, List.of("alice", "bob", "carol"), in("alice"), in("bob"));
        when(callRepository.addParticipant(eq("call-1"), any(), eq(MAX_PARTICIPANTS))).thenReturn(Mono.just(joined));
        Call active = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), in("alice"), in("bob"));
        when(callRepository.markActive("call-1")).thenReturn(Mono.just(active));
        when(iceServerProvider.iceServers()).thenReturn(Mono.just(ICE));

        StepVerifier.create(service.join("bob", "call-1"))
                .expectNext(new CallJoin(active, "session-bob", ICE))
                .expectComplete().verify(Duration.ofSeconds(10));

        ArgumentCaptor<CallParticipant> added = ArgumentCaptor.forClass(CallParticipant.class);
        verify(callRepository).addParticipant(eq("call-1"), added.capture(), eq(MAX_PARTICIPANTS));
        assertThat(added.getValue()).isEqualTo(new CallParticipant("bob", "session-bob", NOW, NOW, List.of()));
        assertThat(broadcastEvents(1)).singleElement()
                .isEqualTo(new CallEvent(CallEventType.PARTICIPANT_JOINED, "alice", "bob", active));
    }

    @Test
    void join_shouldRefuseWithAccountDeleted_withoutOpeningASession_whenTheCallerIsDeleted() {
        when(markers.isDeleted("bob")).thenReturn(Mono.just(true));

        StepVerifier.create(service.join("bob", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.ACCOUNT_DELETED)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(callRepository, mediaRelay, callBroadcaster);
    }

    @Test
    void join_shouldLeaveARingingCallRinging_whenTheCallerIsTheFirstIn() {
        Call ringing = call(CallStatus.RINGING, List.of("alice", "bob"));
        when(callRepository.findById("call-1", "alice")).thenReturn(Mono.just(ringing));
        memberOfConversation("alice");
        when(mediaRelay.newSession()).thenReturn(Mono.just("session-alice"));
        Call joined = call(CallStatus.RINGING, List.of("alice", "bob"), in("alice"));
        when(callRepository.addParticipant(eq("call-1"), any(), anyInt())).thenReturn(Mono.just(joined));
        when(iceServerProvider.iceServers()).thenReturn(Mono.just(ICE));

        StepVerifier.create(service.join("alice", "call-1"))
                .assertNext(join -> assertThat(join.call().status()).isEqualTo(CallStatus.RINGING))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository, never()).markActive(anyString());
        verifyNoInteractions(callBroadcaster);
    }

    @Test
    void join_shouldRefuse_whenTheCallIsFull_withoutOpeningASession() {
        Call full = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol", "dan"), in("alice"), in("bob"), in("carol"));
        when(callRepository.findById("call-1", "dan")).thenReturn(Mono.just(full));
        memberOfConversation("dan");

        StepVerifier.create(service.join("dan", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_FULL)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(mediaRelay);
    }

    @Test
    void join_shouldLetSomeoneAlreadyInRejoin_evenWhenTheCallIsFull() {
        Call full = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), in("alice"), in("bob"), in("carol"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(full));
        memberOfConversation("bob");
        when(mediaRelay.newSession()).thenReturn(Mono.just("session-bob-2"));
        when(callRepository.addParticipant(eq("call-1"), any(), anyInt())).thenReturn(Mono.just(full));
        when(iceServerProvider.iceServers()).thenReturn(Mono.just(ICE));

        StepVerifier.create(service.join("bob", "call-1"))
                .assertNext(join -> assertThat(join.sessionId()).isEqualTo("session-bob-2"))
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void join_shouldRefuse_whenTheCallFilledUpBetweenTheCheckAndTheWrite() {
        Call open = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol", "dan"), in("alice"), in("bob"));
        Call filled = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol", "dan"), in("alice"), in("bob"), in("carol"));
        when(callRepository.findById("call-1", "dan")).thenReturn(Mono.just(open), Mono.just(filled));
        memberOfConversation("dan");
        when(mediaRelay.newSession()).thenReturn(Mono.just("session-dan"));
        when(callRepository.addParticipant(eq("call-1"), any(), anyInt())).thenReturn(Mono.empty());

        StepVerifier.create(service.join("dan", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_FULL)).verify(Duration.ofSeconds(10));
    }

    @Test
    void join_shouldRefuse_whenTheCallEndedBetweenTheCheckAndTheWrite() {
        Call open = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"));
        Call ended = call(CallStatus.ENDED, List.of("alice", "bob"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(open), Mono.just(ended));
        memberOfConversation("bob");
        when(mediaRelay.newSession()).thenReturn(Mono.just("session-bob"));
        when(callRepository.addParticipant(eq("call-1"), any(), anyInt())).thenReturn(Mono.empty());

        StepVerifier.create(service.join("bob", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));
    }

    @Test
    void join_shouldRefuse_whenTheCallHasEnded() {
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"))));

        StepVerifier.create(service.join("bob", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));
    }

    @Test
    void join_shouldRefuse_aMemberWhoLeftTheConversationSinceTheCallStarted() {
        when(callRepository.findById("call-1", "carol"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), in("alice"))));
        when(chatRepository.findById("conv-1", "carol")).thenReturn(Mono.empty());

        StepVerifier.create(service.join("carol", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(mediaRelay);
    }

    @Test
    void join_shouldRefuse_someoneWhoWasNeverAMember() {
        when(callRepository.findById("call-1", "mallory")).thenReturn(Mono.empty());

        StepVerifier.create(service.join("mallory", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));
    }

    // --- publish -------------------------------------------------------------------------

    @Test
    void publish_shouldUseTheCallersOwnSession_recordTheTracks_andTellTheOthers() {
        Call active = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(active));
        SessionDescription offer = new SessionDescription("offer", "v=0 offer");
        List<LocalTrack> tracks = List.of(new LocalTrack("0", "audio"), new LocalTrack("1", "camera"));
        SessionDescription answer = SessionDescription.answer("v=0 answer");
        when(mediaRelay.publish("session-bob", offer, tracks)).thenReturn(Mono.just(answer));
        Call published = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob", "audio", "camera"));
        when(callRepository.addTracks("call-1", "bob", List.of("audio", "camera"))).thenReturn(Mono.just(published));

        StepVerifier.create(service.publish("bob", "call-1", offer, tracks)).expectNext(answer).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(broadcastEvents(1)).singleElement()
                .isEqualTo(new CallEvent(CallEventType.TRACKS_PUBLISHED, "alice", "bob", published));
    }

    @Test
    void publish_shouldRefuse_aMemberWhoHasNotJoined() {
        when(callRepository.findById("call-1", "carol"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "carol"), in("alice"))));

        StepVerifier.create(service.publish("carol", "call-1", new SessionDescription("offer", "x"), List.of()))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(mediaRelay);
    }

    // --- subscribe -----------------------------------------------------------------------

    @Test
    void subscribe_shouldAskForTracksByThePublishersSession_andNameThemByPersonInTheAnswer() {
        Call active = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice", "audio", "camera"), in("bob"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(active));
        SessionDescription offer = new SessionDescription("offer", "v=0 relay");
        when(mediaRelay.subscribe(eq("session-bob"), anyList())).thenReturn(Mono.just(new RelaySubscription(offer, true,
                List.of(new RemoteTrack("session-alice", "camera", VideoLayer.HALF, "3")))));

        StepVerifier.create(service.subscribe("bob", "call-1",
                        List.of(new TrackRequest("alice", "camera", VideoLayer.HALF))))
                .assertNext(subscription -> {
                    assertThat(subscription.offer()).isEqualTo(offer);
                    assertThat(subscription.requiresRenegotiation()).isTrue();
                    assertThat(subscription.tracks()).containsExactly(
                            new CallSubscription.ReceivedTrack("3", "alice", "camera"));
                })
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(mediaRelay).subscribe("session-bob",
                List.of(new RemoteTrack("session-alice", "camera", VideoLayer.HALF, null)));
    }

    @Test
    void subscribe_shouldRefuse_aTrackNobodyPublishesInThisCall() {
        Call active = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice", "audio"), in("bob"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(active));

        StepVerifier.create(service.subscribe("bob", "call-1", List.of(new TrackRequest("alice", "camera", null))))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_TRACK_DENIED)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(mediaRelay);
    }

    @Test
    void subscribe_shouldRefuse_aTrackFromSomeoneOutsideTheCall() {
        Call active = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice", "audio"), in("bob"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(active));

        StepVerifier.create(service.subscribe("bob", "call-1", List.of(new TrackRequest("eve", "audio", null))))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_TRACK_DENIED)).verify(Duration.ofSeconds(10));
    }

    // --- renegotiate, layers, unsubscribe ------------------------------------------------

    @Test
    void renegotiate_shouldAnswerOnTheCallersOwnSession() {
        when(callRepository.findById("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob"))));
        SessionDescription answer = SessionDescription.answer("v=0");
        when(mediaRelay.renegotiate("session-bob", answer)).thenReturn(Mono.empty());

        StepVerifier.create(service.renegotiate("bob", "call-1", answer)).expectComplete().verify(Duration.ofSeconds(10));

        verify(mediaRelay).renegotiate("session-bob", answer);
    }

    @Test
    void renegotiate_shouldRefuse_whenTheCallHasEnded() {
        when(callRepository.findById("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"), in("bob"))));

        StepVerifier.create(service.renegotiate("bob", "call-1", SessionDescription.answer("v=0")))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));
    }

    @Test
    void chooseLayers_shouldSwitchTheReceivedCamera_byItsPublishersSession() {
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(
                call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice", "camera"), in("bob"))));
        when(mediaRelay.updateLayers(eq("session-bob"), anyList())).thenReturn(Mono.empty());

        StepVerifier.create(service.chooseLayers("bob", "call-1",
                List.of(new LayerChoice("3", "alice", "camera", VideoLayer.QUARTER)))).expectComplete().verify(Duration.ofSeconds(10));

        verify(mediaRelay).updateLayers("session-bob",
                List.of(new RemoteTrack("session-alice", "camera", VideoLayer.QUARTER, "3")));
    }

    @Test
    void chooseLayers_shouldRefuse_aCameraNotPublishedInThisCall() {
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(
                call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice", "audio"), in("bob"))));

        StepVerifier.create(service.chooseLayers("bob", "call-1",
                        List.of(new LayerChoice("3", "alice", "camera", VideoLayer.FULL))))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_TRACK_DENIED)).verify(Duration.ofSeconds(10));
    }

    @Test
    void unsubscribe_shouldCloseTheTransceiversOnTheCallersOwnSession() {
        when(callRepository.findById("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob"))));
        when(mediaRelay.closeTracks("session-bob", List.of("3", "4"))).thenReturn(Mono.empty());

        StepVerifier.create(service.unsubscribe("bob", "call-1", List.of("3", "4"))).expectComplete().verify(Duration.ofSeconds(10));

        verify(mediaRelay).closeTracks("session-bob", List.of("3", "4"));
    }

    @Test
    void unsubscribe_shouldRefuse_aMemberWhoHasNotJoined() {
        when(callRepository.findById("call-1", "carol"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "carol"), in("alice"))));

        StepVerifier.create(service.unsubscribe("carol", "call-1", List.of("3")))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));
    }

    // --- heartbeat -----------------------------------------------------------------------

    @Test
    void heartbeat_shouldRecordThatTheParticipantIsStillThere() {
        when(callRepository.touch("call-1", "bob", NOW)).thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("bob"), in("bob"))));

        StepVerifier.create(service.heartbeat("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void heartbeat_shouldRefuse_someoneNotInTheCall() {
        when(callRepository.touch("call-1", "bob", NOW)).thenReturn(Mono.empty());

        StepVerifier.create(service.heartbeat("bob", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));
    }

    // --- leave ---------------------------------------------------------------------------

    @Test
    void leave_shouldTellThoseStillIn_whenOthersRemain() {
        Call remaining = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), in("alice"), in("carol"));
        when(callRepository.removeParticipant("call-1", "bob")).thenReturn(Mono.just(remaining));

        StepVerifier.create(service.leave("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository, never()).end(anyString(), any(), any());
        assertThat(broadcastEvents(2)).extracting(CallEvent::type, CallEvent::recipientId).containsExactly(
                tuple(CallEventType.PARTICIPANT_LEFT, "alice"),
                tuple(CallEventType.PARTICIPANT_LEFT, "carol"));
    }

    @Test
    void leave_shouldEndAnAnsweredCall_whenTheLastOneLeaves() {
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"))));
        Call ended = call(CallStatus.ENDED, List.of("alice", "bob"));
        when(callRepository.end("call-1", CallStatus.ENDED, NOW)).thenReturn(Mono.just(ended));

        StepVerifier.create(service.leave("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(broadcastEvents(2)).extracting(CallEvent::type, CallEvent::recipientId).containsExactly(
                tuple(CallEventType.ENDED, "alice"),
                tuple(CallEventType.ENDED, "bob"));

        assertThat(activity.callsEnded).hasSize(1);
    }

    @Test
    void leave_shouldMarkTheCallMissed_whenTheCallerHangsUpBeforeAnyoneAnswered() {
        when(callRepository.removeParticipant("call-1", "alice"))
                .thenReturn(Mono.just(call(CallStatus.RINGING, List.of("alice", "bob"))));
        when(callRepository.end("call-1", CallStatus.MISSED, NOW))
                .thenReturn(Mono.just(call(CallStatus.MISSED, List.of("alice", "bob"))));

        StepVerifier.create(service.leave("alice", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).end("call-1", CallStatus.MISSED, NOW);

        assertThat(activity.callsEnded).isEmpty();
    }

    @Test
    void leave_shouldDoNothing_whenTheCallerWasNotInTheCall() {
        when(callRepository.removeParticipant("call-1", "bob")).thenReturn(Mono.empty());

        StepVerifier.create(service.leave("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(callBroadcaster);
    }

    // --- removeUser ----------------------------------------------------------------------

    /** Bob deleted his account: every live call that involves him is found, ended calls are stripped of him. */
    private void bobIsIn(Call... live) {
        when(callRepository.findLiveInvolving("bob")).thenReturn(Flux.just(live));
        when(callRepository.removeFromEndedCalls("bob")).thenReturn(Mono.empty());
    }

    @Test
    void removeUser_shouldLeaveEveryLiveCallTheUserIsIn_andTellThoseStillIn() {
        bobIsIn(call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), in("alice"), in("bob"), in("carol")));
        when(mediaRelay.closeSession("session-bob")).thenReturn(Mono.empty());
        when(callRepository.removeParticipant("call-1", "bob")).thenReturn(Mono.just(
                call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), in("alice"), in("carol"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository, never()).end(anyString(), any(), any());
        assertThat(broadcastEvents(2)).extracting(CallEvent::type, CallEvent::recipientId, CallEvent::actorId)
                .containsExactly(
                        tuple(CallEventType.PARTICIPANT_LEFT, "alice", "bob"),
                        tuple(CallEventType.PARTICIPANT_LEFT, "carol", "bob"));
    }

    @Test
    @DisplayName("the user is taken out before its relay session closes: a device still connected cannot reopen tracks")
    void removeUser_shouldCloseTheRelaySession_afterLeaving() {
        bobIsIn(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob")));
        when(mediaRelay.closeSession("session-bob")).thenReturn(Mono.empty());
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        InOrder order = inOrder(mediaRelay, callRepository);
        order.verify(callRepository).removeParticipant("call-1", "bob");
        order.verify(mediaRelay).closeSession("session-bob");
    }

    @Test
    @DisplayName("a participant that cannot be taken out fails the erasure, which runs again; its session is left for then")
    void removeUser_shouldFail_withoutClosingTheSession_whenTheUserCannotBeTakenOut() {
        when(callRepository.findLiveInvolving("bob")).thenReturn(Flux.just(
                call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob"))));
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(service.removeUser("bob"))
                .consumeErrorWith(error -> assertThat(((TechnicalException) error).getErrorCode())
                        .isEqualTo(TechnicalErrorCode.STORE_UNAVAILABLE)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(mediaRelay);
        verify(callRepository, never()).removeFromEndedCalls(anyString());
    }

    @Test
    void removeUser_shouldStillLeave_whenTheRelayCannotBeReached() {
        bobIsIn(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob")));
        when(mediaRelay.closeSession("session-bob"))
                .thenReturn(Mono.error(new TechnicalException(TechnicalErrorCode.CALLS_UNAVAILABLE)));
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).removeParticipant("call-1", "bob");
        verify(callRepository).removeFromEndedCalls("bob");
    }

    @Test
    void removeUser_shouldEndTheCall_whenTheUserWasTheLastOneIn() {
        bobIsIn(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("bob")));
        when(mediaRelay.closeSession("session-bob")).thenReturn(Mono.empty());
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"))));
        when(callRepository.end("call-1", CallStatus.ENDED, NOW))
                .thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).end("call-1", CallStatus.ENDED, NOW);
    }

    @Test
    @DisplayName("a call the deleted user rang alone ends ENDED: MISSED would notify a missed call from them")
    void removeUser_shouldEndARingingCallTheUserWasAloneIn_withoutEmittingMissed() {
        Call rangByBob = new Call("call-1", "conv-1", "bob", "Bob", CallMedia.AUDIO, CallStatus.RINGING, NOW, null,
                List.of("alice", "bob"), List.of(in("bob")));
        bobIsIn(rangByBob);
        when(mediaRelay.closeSession("session-bob")).thenReturn(Mono.empty());
        when(callRepository.removeParticipant("call-1", "bob")).thenReturn(Mono.just(new Call("call-1", "conv-1", "bob", "Bob",
                CallMedia.AUDIO, CallStatus.RINGING, NOW, null, List.of("alice", "bob"), List.of())));
        when(callRepository.end("call-1", CallStatus.ENDED, NOW))
                .thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).end("call-1", CallStatus.ENDED, NOW);
        verify(callRepository, never()).end(anyString(), eq(CallStatus.MISSED), any());
    }

    @Test
    void removeUser_shouldEndARingingCallTheUserStartedButNeverJoined_withoutEmittingMissed() {
        Call rangByBob = new Call("call-1", "conv-1", "bob", "Bob", CallMedia.VIDEO, CallStatus.RINGING, NOW, null,
                List.of("alice", "bob"), List.of());
        bobIsIn(rangByBob);
        when(callRepository.end("call-1", CallStatus.ENDED, NOW))
                .thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).end("call-1", CallStatus.ENDED, NOW);
        verify(callRepository, never()).end(anyString(), eq(CallStatus.MISSED), any());
        verify(callRepository, never()).removeParticipant(anyString(), anyString());
        verifyNoInteractions(mediaRelay);
    }

    @Test
    @DisplayName("a private call still ringing the deleted callee stops ringing, ENDED rather than MISSED")
    void removeUser_shouldEndARingingDirectCallToTheUser_withoutEmittingMissed() {
        Call ringingBob = call(CallStatus.RINGING, List.of("alice", "bob"), in("alice"));
        bobIsIn(ringingBob);
        when(callRepository.end("call-1", CallStatus.ENDED, NOW))
                .thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).end("call-1", CallStatus.ENDED, NOW);
        verify(callRepository, never()).end(anyString(), eq(CallStatus.MISSED), any());
        verify(callRepository, never()).removeParticipant(anyString(), anyString());
        verifyNoInteractions(mediaRelay);
    }

    @Test
    @DisplayName("a group call ringing the deleted member goes on ringing the others")
    void removeUser_shouldLeaveARingingGroupCallTheUserIsOnlyRungIn() {
        bobIsIn(call(CallStatus.RINGING, List.of("alice", "bob", "carol"), in("alice")));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository, never()).end(anyString(), any(), any());
        verifyNoInteractions(callBroadcaster, mediaRelay);
    }

    @Test
    @DisplayName("a call the user started and has already left goes on for the others")
    void removeUser_shouldLeaveAnActiveCallTheUserStartedButLeftAlone() {
        Call startedByBob = new Call("call-1", "conv-1", "bob", "Bob", CallMedia.AUDIO, CallStatus.ACTIVE, NOW, null,
                List.of("alice", "bob", "carol"), List.of(in("alice"), in("carol")));
        bobIsIn(startedByBob);

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository, never()).end(anyString(), any(), any());
        verify(callRepository, never()).removeParticipant(anyString(), anyString());
        verifyNoInteractions(callBroadcaster, mediaRelay);
    }

    @Test
    void removeUser_shouldStripTheUserFromEndedCalls_afterLeavingLiveOnes() {
        bobIsIn(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob")));
        when(mediaRelay.closeSession("session-bob")).thenReturn(Mono.empty());
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"))));

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        InOrder order = inOrder(callRepository);
        order.verify(callRepository).removeParticipant("call-1", "bob");
        order.verify(callRepository).removeFromEndedCalls("bob");
    }

    @Test
    void removeUser_shouldDoNothing_whenTheUserIsInNoCall() {
        bobIsIn();

        StepVerifier.create(service.removeUser("bob")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).removeFromEndedCalls("bob");
        verifyNoInteractions(callBroadcaster, mediaRelay);
    }

    @Test
    void removeUser_shouldFail_whenTheCallsCannotBeRead() {
        when(callRepository.findLiveInvolving("bob"))
                .thenReturn(Flux.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(service.removeUser("bob")).expectError(TechnicalException.class).verify(Duration.ofSeconds(10));

        verify(callRepository, never()).removeFromEndedCalls(anyString());
    }

    // --- decline -------------------------------------------------------------------------

    @Test
    void decline_shouldEndARingingDirectCall() {
        Call ringing = call(CallStatus.RINGING, List.of("alice", "bob"), in("alice"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(ringing));
        memberOfConversation("bob");
        Call declined = call(CallStatus.DECLINED, List.of("alice", "bob"));
        when(callRepository.end("call-1", CallStatus.DECLINED, NOW)).thenReturn(Mono.just(declined));

        StepVerifier.create(service.decline("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(broadcastEvents(2)).extracting(CallEvent::type).containsOnly(CallEventType.ENDED);

        assertThat(activity.callsEnded).isEmpty();
    }

    @Test
    void decline_shouldOnlyTellThoseInTheCall_inAGroup() {
        Call ringing = call(CallStatus.RINGING, List.of("alice", "bob", "carol"), in("alice"));
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(ringing));
        memberOfConversation("bob");

        StepVerifier.create(service.decline("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository, never()).end(anyString(), any(), any());
        assertThat(broadcastEvents(1)).singleElement()
                .isEqualTo(new CallEvent(CallEventType.DECLINED, "alice", "bob", ringing));
    }

    @Test
    void decline_shouldRefuseWithAccountDeleted_whenTheCallerIsDeleted() {
        when(markers.isDeleted("bob")).thenReturn(Mono.just(true));

        StepVerifier.create(service.decline("bob", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.ACCOUNT_DELETED)).verify(Duration.ofSeconds(10));

        verifyNoInteractions(callRepository, chatRepository, callBroadcaster, chatService);
    }

    @Test
    void decline_shouldRefuse_aCallThatIsOver() {
        when(callRepository.findById("call-1", "bob")).thenReturn(Mono.just(call(CallStatus.MISSED, List.of("alice", "bob"))));

        StepVerifier.create(service.decline("bob", "call-1"))
                .consumeErrorWith(error -> assertThat(((BusinessException) error).getErrorCode())
                        .isEqualTo(BusinessErrorCode.CALL_NOT_FOUND)).verify(Duration.ofSeconds(10));
    }

    // --- events --------------------------------------------------------------------------

    @Test
    void streamEvents_shouldRelayTheEventsAddressedToTheUser() {
        CallEvent ringing = new CallEvent(CallEventType.RINGING, "bob", "alice", call(CallStatus.RINGING, List.of("alice", "bob")));
        when(callBroadcaster.subscribe("bob")).thenReturn(Flux.just(ringing));

        StepVerifier.create(service.streamEvents("bob")).expectNext(ringing).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void notify_shouldSkipARecipientWhosePushFails_andReachTheOthers() {
        memberOfConversation("alice");
        when(callRepository.findLive("conv-1")).thenReturn(Mono.empty());
        Call created = call(CallStatus.RINGING, List.of("alice", "bob", "carol"));
        when(callRepository.createOrGetLive(any())).thenReturn(Mono.just(created));
        when(callBroadcaster.broadcast(any()))
                .thenReturn(Mono.error(new IllegalStateException("broker down")), Mono.empty());

        StepVerifier.create(service.start("alice", "Alice", "conv-1", CallMedia.AUDIO)).expectNext(created).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(broadcastEvents(2)).extracting(CallEvent::recipientId).containsExactly("bob", "carol");
    }

    // --- end, notices --------------------------------------------------------------------

    @Test
    void end_shouldLeaveTheCallsSummaryInTheConversation_onceAndKeyedByTheCall() {
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"))));
        Call ended = call(CallStatus.ENDED, List.of("alice", "bob"));
        when(callRepository.end("call-1", CallStatus.ENDED, NOW)).thenReturn(Mono.just(ended));
        when(noticeFormatter.format(ended)).thenReturn("📞 Appel vidéo · 12 min");

        StepVerifier.create(service.leave("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        verify(chatService).postConversationNotice("conv-1", "call:call-1", "📞 Appel vidéo · 12 min");
    }

    @Test
    void end_shouldStillEndTheCall_whenTheNoticeCannotBePosted() {
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"))));
        when(callRepository.end("call-1", CallStatus.ENDED, NOW))
                .thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"))));
        when(chatService.postConversationNotice(anyString(), anyString(), anyString()))
                .thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(service.leave("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        verify(callBroadcaster, times(2)).broadcast(any());
    }

    @Test
    void end_shouldPostNothing_whenAnotherInstanceAlreadyEndedTheCall() {
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob"))));
        when(callRepository.end("call-1", CallStatus.ENDED, NOW)).thenReturn(Mono.empty());

        StepVerifier.create(service.leave("bob", "call-1")).expectComplete().verify(Duration.ofSeconds(10));

        verifyNoInteractions(chatService, callBroadcaster);

        assertThat(activity.callsEnded).isEmpty();
    }

    // --- expire --------------------------------------------------------------------------

    @Test
    void expire_shouldMarkUnansweredCallsMissed() {
        Call ringing = call(CallStatus.RINGING, List.of("alice", "bob"), in("alice"));
        when(callRepository.findRingingStartedBefore(NOW.minus(CallService.RING_TIMEOUT))).thenReturn(Flux.just(ringing));
        when(callRepository.findLiveWithParticipantSeenBefore(any())).thenReturn(Flux.empty());
        when(callRepository.findLiveStartedBefore(any())).thenReturn(Flux.empty());
        when(callRepository.end("call-1", CallStatus.MISSED, NOW))
                .thenReturn(Mono.just(call(CallStatus.MISSED, List.of("alice", "bob"))));

        StepVerifier.create(service.expire()).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).end("call-1", CallStatus.MISSED, NOW);
    }

    @Test
    void expire_shouldTakeOutOnlyTheParticipantsWhoWentSilent() {
        Instant silentSince = NOW.minus(CallService.SILENCE_TIMEOUT);
        CallParticipant silent = new CallParticipant("bob", "s-bob", NOW, silentSince.minusSeconds(1), List.of());
        CallParticipant alive = new CallParticipant("carol", "s-carol", NOW, NOW, List.of());
        Call call = call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), silent, alive);
        when(callRepository.findRingingStartedBefore(any())).thenReturn(Flux.empty());
        when(callRepository.findLiveWithParticipantSeenBefore(silentSince)).thenReturn(Flux.just(call));
        when(callRepository.findLiveStartedBefore(any())).thenReturn(Flux.empty());
        when(callRepository.removeParticipant("call-1", "bob"))
                .thenReturn(Mono.just(call(CallStatus.ACTIVE, List.of("alice", "bob", "carol"), alive)));

        StepVerifier.create(service.expire()).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).removeParticipant("call-1", "bob");
        verify(callRepository, never()).removeParticipant("call-1", "carol");
    }

    @Test
    void expire_shouldEndCallsPastTheMaximumDuration() {
        Call longCall = call(CallStatus.ACTIVE, List.of("alice", "bob"), in("alice"), in("bob"));
        when(callRepository.findRingingStartedBefore(any())).thenReturn(Flux.empty());
        when(callRepository.findLiveWithParticipantSeenBefore(any())).thenReturn(Flux.empty());
        when(callRepository.findLiveStartedBefore(NOW.minus(CallService.MAX_DURATION))).thenReturn(Flux.just(longCall));
        when(callRepository.end("call-1", CallStatus.ENDED, NOW))
                .thenReturn(Mono.just(call(CallStatus.ENDED, List.of("alice", "bob"))));

        StepVerifier.create(service.expire()).expectComplete().verify(Duration.ofSeconds(10));

        verify(callRepository).end("call-1", CallStatus.ENDED, NOW);
    }

    @Test
    @DisplayName("an answered call reports how long it ran, from its first ring to the end")
    void end_shouldReportTheCallsDuration_fromItsFirstRing() {
        Call fiveMinutesOld = new Call("call-1", "conv-1", "alice", "Alice", CallMedia.VIDEO, CallStatus.ACTIVE,
                NOW.minus(Duration.ofMinutes(5)), null, List.of("alice", "bob"), List.of());
        when(callRepository.end("call-1", CallStatus.ENDED, NOW)).thenReturn(Mono.just(new Call("call-1", "conv-1",
                "alice", "Alice", CallMedia.VIDEO, CallStatus.ENDED, NOW.minus(Duration.ofMinutes(5)), NOW,
                List.of("alice", "bob"), List.of())));

        StepVerifier.create(service.end(fiveMinutesOld, CallStatus.ENDED, "alice"))
                .expectNextCount(1).expectComplete().verify(Duration.ofSeconds(10));

        assertThat(activity.callsEnded).containsExactly(Duration.ofMinutes(5));
    }
}
