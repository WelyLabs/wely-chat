package com.calendar.chat.application.mappers;

import com.calendar.chat.application.dtos.LiveMessageDTO;
import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.Photo;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class LiveMessageMapperTest {

    private static final Instant SENT_AT = Instant.parse("2026-10-13T20:00:00Z");

    private final LiveMessageMapper mapper = Mappers.getMapper(LiveMessageMapper.class);

    @Test
    void toDTO_shouldFlattenTheMessageNextToItsKind() {
        Photo photo = new Photo("chat/c1/m1.jpg", 1600, 1200, "https://signed", false);
        LiveMessage live = new LiveMessage(new Message("m1", "alice", "Alice", "bob", "c1", "Podium", SENT_AT, photo),
                BroadcastKind.UPDATED);

        assertThat(mapper.toDTO(live)).isEqualTo(
                new LiveMessageDTO("m1", "alice", "Alice", "bob", "c1", "Podium", SENT_AT, photo, BroadcastKind.UPDATED));
    }

    @Test
    void toDTO_shouldMapNullToNull() {
        assertThat(mapper.toDTO(null)).isNull();
    }

    @Test
    void toDTO_shouldSerializeTheKindAsAString_besideTheMessageFields() {
        LiveMessage live = new LiveMessage(new Message("m1", "alice", "Alice", "bob", "c1", "", SENT_AT, Photo.REMOVED),
                BroadcastKind.UPDATED);

        String json = JsonMapper.builder().build().writeValueAsString(mapper.toDTO(live));

        assertThat(json)
                .contains("\"id\":\"m1\"")
                .contains("\"conversationId\":\"c1\"")
                .contains("\"photo\":{\"key\":null,\"width\":0,\"height\":0,\"url\":null,\"removed\":true}")
                .contains("\"kind\":\"UPDATED\"");
    }
}
