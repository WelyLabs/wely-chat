package com.calendar.chat.application.mappers;

import com.calendar.chat.application.dtos.CallDTO;
import com.calendar.chat.application.dtos.LayersRequest;
import com.calendar.chat.application.dtos.SubscribeRequest;
import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.domain.models.LayerChoice;
import com.calendar.chat.domain.models.TrackRequest;
import com.calendar.chat.domain.models.VideoLayer;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CallRestMapperTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");

    private final CallRestMapper mapper = Mappers.getMapper(CallRestMapper.class);

    @Test
    void toDTO_shouldKeepWhoPublishesWhat_butNotTheRelaySessions() {
        Call call = new Call("call-1", "conv-1", "alice", CallMedia.AUDIO, CallStatus.RINGING, NOW, null,
                List.of("alice"), List.of(new CallParticipant("alice", "s-secret", NOW, NOW, List.of("audio"))));

        CallDTO dto = mapper.toDTO(call);

        assertThat(dto.participants()).containsExactly(new CallDTO.Participant("alice", NOW, List.of("audio")));
        assertThat(dto.toString()).doesNotContain("s-secret");
    }

    @Test
    void toDomain_shouldReadLayersByName_andLeaveAudioWithout() {
        assertThat(mapper.toDomain(new SubscribeRequest.Track("alice", "camera", "QUARTER")))
                .isEqualTo(new TrackRequest("alice", "camera", VideoLayer.QUARTER));
        assertThat(mapper.toDomain(new SubscribeRequest.Track("alice", "audio", null)).layer()).isNull();
        assertThat(mapper.toDomain(new LayersRequest.Choice("3", "alice", "camera", "FULL")))
                .isEqualTo(new LayerChoice("3", "alice", "camera", VideoLayer.FULL));
    }

    @Test
    void toDomain_shouldRejectAnUnknownLayer() {
        assertThatThrownBy(() -> mapper.toDomain(new SubscribeRequest.Track("alice", "camera", "ULTRA")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
