package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallMedia;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallStatus;
import com.calendar.chat.infrastructure.persistence.models.entities.CallEntity;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CallPersistenceMapperTest {

    private static final Instant NOW = Instant.parse("2026-10-15T18:00:00Z");

    private final CallPersistenceMapper mapper = Mappers.getMapper(CallPersistenceMapper.class);

    private static Call call(CallStatus status) {
        return new Call("call-1", "conv-1", "alice", "Alice", CallMedia.VIDEO, status, NOW, null, List.of("alice", "bob"),
                List.of(new CallParticipant("alice", "s-1", NOW, NOW, List.of("audio", "camera"))));
    }

    @Test
    void toEntity_shouldMarkALiveCallLive_forTheUniqueIndex() {
        assertThat(mapper.toEntity(call(CallStatus.RINGING)).isLive()).isTrue();
        assertThat(mapper.toEntity(call(CallStatus.ACTIVE)).isLive()).isTrue();
        assertThat(mapper.toEntity(call(CallStatus.ENDED)).isLive()).isFalse();
    }

    @Test
    void toEntity_shouldStoreTheEnumsByName_andEmbedTheParticipants() {
        CallEntity entity = mapper.toEntity(call(CallStatus.ACTIVE));

        assertThat(entity.getStatus()).isEqualTo("ACTIVE");
        assertThat(entity.getMedia()).isEqualTo("VIDEO");
        assertThat(entity.getStartedByName()).isEqualTo("Alice");
        assertThat(entity.getParticipants()).singleElement()
                .satisfies(participant -> assertThat(participant.getTrackNames()).containsExactly("audio", "camera"));
    }

    @Test
    void toDomain_shouldGiveBackTheSameCall() {
        Call call = call(CallStatus.ACTIVE);

        assertThat(mapper.toDomain(mapper.toEntity(call))).isEqualTo(call);
    }

    @Test
    void toDomain_shouldReadANullName_fromACallStoredBeforeIt() {
        CallEntity older = mapper.toEntity(call(CallStatus.ENDED));
        older.setStartedByName(null);

        Call call = mapper.toDomain(older);

        assertThat(call.startedByName()).isNull();
        assertThat(call.startedBy()).isEqualTo("alice");
    }

    @Test
    void toDomain_shouldGiveNull_forNull() {
        assertThat(mapper.toDomain((CallEntity) null)).isNull();
    }
}
