package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageBroadcastMapperTest {

    private static final Instant SENT_AT = Instant.parse("2026-10-13T20:00:00Z");

    private final MessageBroadcastMapper mapper = Mappers.getMapper(MessageBroadcastMapper.class);

    private static Message withPhoto(Photo photo) {
        return new Message("m1", "alice", "Alice", "bob", "c1", "Podium", SENT_AT, photo);
    }

    @Test
    void toBroadcastDTO_shouldFlattenTheMessageAndItsKind() {
        Photo signed = new Photo("chat/c1/m1.jpg", 1600, 1200, "https://signed", false);

        MessageBroadcastDTO dto = mapper.toBroadcastDTO(new LiveMessage(withPhoto(signed), BroadcastKind.UPDATED));

        assertThat(dto).isEqualTo(new MessageBroadcastDTO("m1", "alice", "Alice", "bob", "c1", "Podium", SENT_AT,
                new MessageBroadcastDTO.PhotoDTO("chat/c1/m1.jpg", 1600, 1200, false), "UPDATED"));
    }

    @Test
    void toLiveMessage_shouldRoundTripTheKind_andDropTheUrl() {
        LiveMessage live = new LiveMessage(withPhoto(Photo.REMOVED), BroadcastKind.UPDATED);

        assertThat(mapper.toLiveMessage(mapper.toBroadcastDTO(live))).isEqualTo(live);
    }

    @Test
    void toLiveMessage_shouldReadARecordWithoutKindAsNew() {
        MessageBroadcastDTO legacy = new MessageBroadcastDTO("m1", "alice", "Alice", "bob", "c1", "hi", SENT_AT, null, null);

        LiveMessage live = mapper.toLiveMessage(legacy);

        assertThat(live.kind()).isEqualTo(BroadcastKind.NEW);
        assertThat(live.message()).isEqualTo(new Message("m1", "alice", "Alice", "bob", "c1", "hi", SENT_AT));
    }

    @Test
    void toLiveMessage_shouldRefuseAnUnknownKind() {
        // Thrown inside the consumer's per-record dispatch: one dropped record, not a deaf pod.
        MessageBroadcastDTO future = new MessageBroadcastDTO("m1", "alice", "Alice", "bob", "c1", "hi", SENT_AT, null, "DELETED");

        assertThatThrownBy(() -> mapper.toLiveMessage(future)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toPhoto_shouldLeaveTheUrlUnset() {
        assertThat(mapper.toPhoto(new MessageBroadcastDTO.PhotoDTO("k", 1, 2, false))).isEqualTo(new Photo("k", 1, 2, null, false));
    }

    @Test
    void mappers_shouldMapNullToNull() {
        assertThat(mapper.toBroadcastDTO(null)).isNull();
        assertThat(mapper.toLiveMessage(null)).isNull();
        assertThat(mapper.toMessage(null)).isNull();
        assertThat(mapper.toPhotoDTO(null)).isNull();
    }
}
