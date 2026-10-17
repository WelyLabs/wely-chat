package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallEventType;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CallEventBroadcastMapperTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");

    private final CallEventBroadcastMapper mapper = Mappers.getMapper(CallEventBroadcastMapper.class);

    private static CallEvent event() {
        Call call = new Call("call-1", "conv-1", "alice", "Alice", CallMedia.VIDEO, CallStatus.ACTIVE, NOW, null,
                List.of("alice", "bob"), List.of(new CallParticipant("alice", "s-1", NOW, NOW, List.of("audio"))));
        return new CallEvent(CallEventType.TRACKS_PUBLISHED, "bob", "alice", call);
    }

    @Test
    void toBroadcastDTO_shouldNameTheEnumsForTheWire() {
        CallEventBroadcastDTO dto = mapper.toBroadcastDTO(event());

        assertThat(dto.type()).isEqualTo("TRACKS_PUBLISHED");
        assertThat(dto.call().status()).isEqualTo("ACTIVE");
        assertThat(dto.call().startedByName()).isEqualTo("Alice");
        assertThat(dto.call().participants()).singleElement()
                .satisfies(participant -> assertThat(participant.sessionId()).isEqualTo("s-1"));
    }

    @Test
    void toCallEvent_shouldGiveBackTheSameEvent() {
        assertThat(mapper.toCallEvent(mapper.toBroadcastDTO(event()))).isEqualTo(event());
    }

    @Test
    void toCall_shouldReadANullName_fromAnInstanceThatPredatesIt() {
        CallEventBroadcastDTO.CallDTO older = new CallEventBroadcastDTO.CallDTO("call-1", "conv-1", "alice", null,
                "AUDIO", "RINGING", NOW, null, List.of("alice", "bob"), List.of());

        Call call = mapper.toCall(older);

        assertThat(call.startedByName()).isNull();
        assertThat(call.startedBy()).isEqualTo("alice");
    }
}
