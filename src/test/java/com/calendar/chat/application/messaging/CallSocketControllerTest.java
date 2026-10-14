package com.calendar.chat.application.messaging;

import com.calendar.chat.application.mappers.CallRestMapper;
import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallEventType;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.services.CallService;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.security.oauth2.jwt.Jwt;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CallSocketControllerTest {

    @Test
    void streamEvents_shouldStreamTheTokensUsersEvents() {
        CallService callService = mock(CallService.class);
        Jwt jwt = mock(Jwt.class);
        when(jwt.getClaimAsString("businessId")).thenReturn("bob");
        Call call = Call.ringing("conv-1", "alice", CallMedia.VIDEO, Instant.EPOCH, List.of("alice", "bob"));
        when(callService.streamEvents("bob")).thenReturn(Flux.just(new CallEvent(CallEventType.RINGING, "bob", "alice", call)));

        CallSocketController controller = new CallSocketController(callService, Mappers.getMapper(CallRestMapper.class));

        StepVerifier.create(controller.streamEvents(jwt))
                .assertNext(frame -> {
                    assertThat(frame.type()).isEqualTo("RINGING");
                    assertThat(frame.actorId()).isEqualTo("alice");
                    assertThat(frame.call().media()).isEqualTo("VIDEO");
                })
                .verifyComplete();
    }
}
