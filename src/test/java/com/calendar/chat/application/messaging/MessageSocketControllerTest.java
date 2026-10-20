package com.calendar.chat.application.messaging;

import static org.mockito.Mockito.lenient;
import com.calendar.chat.domain.services.PhotoService;
import com.calendar.chat.application.dtos.ChatInputDTO;
import com.calendar.chat.application.dtos.LiveMessageDTO;
import com.calendar.chat.application.mappers.LiveMessageMapper;
import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageType;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.DeletedUserMarkers;
import com.calendar.chat.domain.ports.PresenceRepository;
import com.calendar.chat.domain.services.AccountGuard;
import com.calendar.chat.domain.services.ChatService;
import com.calendar.chat.domain.services.LiveSessions;
import com.calendar.chat.domain.services.PresenceService;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import com.calendar.chat.exception.TechnicalErrorCode;
import com.calendar.chat.exception.TechnicalException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mapstruct.factory.Mappers;
import org.springframework.security.oauth2.jwt.Jwt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageSocketControllerTest {

    private static final String SENDER_ID = "sender-1";
    private static final String SENDER_NAME = "theo";

    @Mock private ChatService chatService;
    @Mock private PresenceService presenceService;
    @Mock private Jwt jwt;
    @Mock private PhotoService photoService;
    @Mock private LiveSessions liveSessions;

    private MessageSocketController controller;

    @BeforeEach
    void setUp() {
        // Text messages carry no photo: signing hands them back unchanged.
        lenient().when(photoService.signed(any(LiveMessage.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));
        // An active account: the live session relays the stream as is.
        lenient().when(liveSessions.attach(anyString(), any())).thenAnswer(call -> call.getArgument(1));
        controller = new MessageSocketController(chatService, presenceService, photoService, liveSessions,
                Mappers.getMapper(LiveMessageMapper.class));
    }

    @Test
    @DisplayName("the sender comes from the token, not from the client payload")
    void sendMessage_shouldTakeTheSenderFromTheToken() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(jwt.getClaimAsString("preferred_username")).thenReturn(SENDER_NAME);
        when(chatService.sendMessage(any(Message.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));

        // The client claims someone else's name in senderUsername: ignored.
        ChatInputDTO input = new ChatInputDTO("attacker-claimed-name", "receiver-1", "conv-1", "hello");

        StepVerifier.create(controller.sendMessage(jwt, input)).expectNextCount(1).expectComplete().verify(Duration.ofSeconds(10));

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(chatService).sendMessage(sent.capture());

        assertThat(sent.getValue().senderId()).isEqualTo(SENDER_ID);
        assertThat(sent.getValue().senderName()).isEqualTo(SENDER_NAME);
        assertThat(sent.getValue().senderName()).isNotEqualTo("attacker-claimed-name");
    }

    @Test
    void sendMessage_shouldCarryTheClientPayloadFields() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(jwt.getClaimAsString("preferred_username")).thenReturn(SENDER_NAME);
        when(chatService.sendMessage(any(Message.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));

        controller.sendMessage(jwt, new ChatInputDTO(null, "receiver-1", "conv-1", "hello")).block();

        ArgumentCaptor<Message> sent = ArgumentCaptor.forClass(Message.class);
        verify(chatService).sendMessage(sent.capture());

        Message message = sent.getValue();
        assertThat(message.receiverId()).isEqualTo("receiver-1");
        assertThat(message.conversationId()).isEqualTo("conv-1");
        assertThat(message.content()).isEqualTo("hello");
        assertThat(message.id()).isNull();          // assigned by the persistence layer
        assertThat(message.timestamp()).isNotNull();
    }

    @Test
    @DisplayName("the stream only delivers messages addressed to the caller")
    void streamMessage_shouldScopeTheStreamToTheCaller() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        Instant at = Instant.now();
        Message mine = new Message("m-1", "other", "Other", SENDER_ID, "conv-1", "hi", at);
        when(chatService.streamMessages(SENDER_ID)).thenReturn(Flux.just(new LiveMessage(mine, BroadcastKind.NEW)));
        when(presenceService.track(eq(SENDER_ID), any())).thenAnswer(call -> call.getArgument(1));

        StepVerifier.create(controller.streamMessage(jwt))
                .expectNext(new LiveMessageDTO("m-1", "other", "Other", SENDER_ID, "conv-1", "hi", at, null, MessageType.TEXT, BroadcastKind.NEW))
                .expectComplete().verify(Duration.ofSeconds(10));

        verify(chatService).streamMessages(SENDER_ID);
    }

    @Test
    @DisplayName("the caller counts as online while their stream is open")
    void streamMessage_shouldTrackTheCallersPresence() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        Flux<LiveMessage> stream = Flux.empty();
        when(chatService.streamMessages(SENDER_ID)).thenReturn(stream);
        // The stream is wrapped to sign photo URLs on the way out: what reaches track is that wrapper.
        when(presenceService.track(eq(SENDER_ID), any())).thenReturn(Flux.empty());

        StepVerifier.create(controller.streamMessage(jwt)).expectComplete().verify(Duration.ofSeconds(10));

        verify(presenceService).track(eq(SENDER_ID), any());
    }

    @Test
    @DisplayName("each frame tells the client whether to append it or replace the message with the same id")
    void streamMessage_shouldExposeTheKindAndTheSignedPhoto() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        Instant at = Instant.now();
        Message removed = new Message("m-1", "other", "Other", SENDER_ID, "conv-1", "Podium", at, Photo.REMOVED);
        Message stored = new Message("m-2", "other", "Other", SENDER_ID, "conv-1", "", at, Photo.stored("chat/conv-1/m-2.jpg", 10, 10));
        Photo signedPhoto = new Photo("chat/conv-1/m-2.jpg", 10, 10, "https://signed", false);
        when(chatService.streamMessages(SENDER_ID)).thenReturn(Flux.just(
                new LiveMessage(removed, BroadcastKind.UPDATED), new LiveMessage(stored, BroadcastKind.NEW)));
        when(photoService.signed(any(LiveMessage.class))).thenAnswer(call -> {
            LiveMessage live = call.getArgument(0);
            return Mono.just(live.message().hasStoredPhoto()
                    ? new LiveMessage(new Message("m-2", "other", "Other", SENDER_ID, "conv-1", "", at, signedPhoto), live.kind())
                    : live);
        });
        when(presenceService.track(eq(SENDER_ID), any())).thenAnswer(call -> call.getArgument(1));

        StepVerifier.create(controller.streamMessage(jwt))
                .assertNext(frame -> {
                    assertThat(frame.kind()).isEqualTo(BroadcastKind.UPDATED);
                    assertThat(frame.photo()).isEqualTo(Photo.REMOVED);
                })
                .assertNext(frame -> {
                    assertThat(frame.kind()).isEqualTo(BroadcastKind.NEW);
                    assertThat(frame.photo().url()).isEqualTo("https://signed");
                })
                .expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("the tracked stream is what the live session holds, so deleting the account ends it")
    void streamMessage_shouldAttachTheTrackedStreamToTheCallersLiveSession() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(chatService.streamMessages(SENDER_ID)).thenReturn(Flux.empty());
        Flux<Object> tracked = Flux.empty();
        when(presenceService.track(eq(SENDER_ID), any())).thenReturn(tracked);

        StepVerifier.create(controller.streamMessage(jwt)).expectComplete().verify(Duration.ofSeconds(10));

        verify(liveSessions).attach(SENDER_ID, tracked);
    }

    @Test
    void streamMessage_shouldFail_whenLiveSessionsRefusesTheCaller() {
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(chatService.streamMessages(SENDER_ID)).thenReturn(Flux.never());
        when(presenceService.track(eq(SENDER_ID), any())).thenReturn(Flux.never());
        when(liveSessions.attach(eq(SENDER_ID), any())).thenReturn(Flux.error(new BusinessException(BusinessErrorCode.ACCOUNT_DELETED)));

        StepVerifier.create(controller.streamMessage(jwt))
                .expectErrorMatches(error -> error instanceof BusinessException business
                        && business.getErrorCode() == BusinessErrorCode.ACCOUNT_DELETED)
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("a stream refused for a deleted account, or while the markers cannot be read, leaves no presence behind")
    void streamMessage_shouldLeaveNothingForTheHeartbeat_whenLiveSessionsRefusesTheStream() {
        DeletedUserMarkers markers = mock(DeletedUserMarkers.class);
        PresenceRepository presenceRepository = mock(PresenceRepository.class);
        AccountGuard guard = new AccountGuard(markers);
        PresenceService presence = new PresenceService(presenceRepository, mock(ChatRepository.class), guard, "pod-1",
                Clock.systemUTC());
        MessageSocketController wired = new MessageSocketController(chatService, presence, photoService,
                new LiveSessions(guard), Mappers.getMapper(LiveMessageMapper.class));
        when(jwt.getClaimAsString("businessId")).thenReturn(SENDER_ID);
        when(chatService.streamMessages(SENDER_ID)).thenReturn(Flux.never());
        when(markers.isDeleted(SENDER_ID)).thenReturn(Mono.just(true),
                Mono.error(new TechnicalException(TechnicalErrorCode.STORE_UNAVAILABLE)));

        StepVerifier.create(wired.streamMessage(jwt))
                .expectErrorMatches(error -> error instanceof BusinessException business
                        && business.getErrorCode() == BusinessErrorCode.ACCOUNT_DELETED)
                .verify(Duration.ofSeconds(10));
        StepVerifier.create(wired.streamMessage(jwt))
                .expectErrorMatches(error -> error instanceof TechnicalException technical
                        && technical.getErrorCode() == TechnicalErrorCode.STORE_UNAVAILABLE)
                .verify(Duration.ofSeconds(10));
        StepVerifier.create(presence.heartbeat()).expectComplete().verify(Duration.ofSeconds(10));

        verify(presenceRepository, never()).connect(anyString(), anyString(), anyString(), any());
        verify(presenceRepository, never()).refresh(any(), any());
    }
}
