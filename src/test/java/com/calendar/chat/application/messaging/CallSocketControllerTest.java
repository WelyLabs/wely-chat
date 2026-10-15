package com.calendar.chat.application.messaging;

import com.calendar.chat.application.mappers.CallRestMapper;
import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallEventType;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.services.CallService;
import com.calendar.chat.domain.services.LiveSessions;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.security.oauth2.jwt.Jwt;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CallSocketControllerTest {

    private final CallService callService = mock(CallService.class);
    private final LiveSessions liveSessions = mock(LiveSessions.class);
    private final Jwt jwt = mock(Jwt.class);
    private CallSocketController controller;

    @BeforeEach
    void setUp() {
        when(jwt.getClaimAsString("businessId")).thenReturn("bob");
        controller = new CallSocketController(callService, liveSessions, Mappers.getMapper(CallRestMapper.class));
    }

    @Test
    void streamEvents_shouldAttachTheStreamToTheCallersLiveSession() {
        Call call = Call.ringing("conv-1", "alice", CallMedia.VIDEO, Instant.EPOCH, List.of("alice", "bob"));
        when(callService.streamEvents("bob")).thenReturn(Flux.just(new CallEvent(CallEventType.RINGING, "bob", "alice", call)));
        when(liveSessions.attach(eq("bob"), any())).thenAnswer(invocation -> invocation.getArgument(1));

        StepVerifier.create(controller.streamEvents(jwt))
                .assertNext(frame -> {
                    assertThat(frame.type()).isEqualTo("RINGING");
                    assertThat(frame.actorId()).isEqualTo("alice");
                    assertThat(frame.call().media()).isEqualTo("VIDEO");
                })
                .verifyComplete();
        verify(liveSessions).attach(eq("bob"), any());
    }

    @Test
    void streamEvents_shouldFail_whenLiveSessionsRefusesTheCaller() {
        when(callService.streamEvents("bob")).thenReturn(Flux.never());
        when(liveSessions.attach(eq("bob"), any())).thenReturn(Flux.error(new ChatException(ChatErrorCode.ACCOUNT_DELETED)));

        StepVerifier.create(controller.streamEvents(jwt))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.ACCOUNT_DELETED)
                .verify();
    }
}
