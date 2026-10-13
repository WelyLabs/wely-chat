package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationPhotoEntity;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ConversationPhotoMapperTest {

    private final ConversationPhotoMapper mapper = Mappers.getMapper(ConversationPhotoMapper.class);

    @Test
    void toEntity_shouldFlattenThePhoto_andToDomainRebuildIt_withoutAUrl() {
        ConversationPhoto photo = new ConversationPhoto("c1", "m1", "alice",
                new Photo("chat/c1/m1.jpg", 1600, 1200, "https://signed", false), Instant.parse("2026-10-13T20:00:00Z"));

        ConversationPhotoEntity entity = mapper.toEntity(photo);

        assertThat(entity.getKey()).isEqualTo("chat/c1/m1.jpg");
        assertThat(entity.getWidth()).isEqualTo(1600);
        assertThat(mapper.toDomain(entity)).isEqualTo(new ConversationPhoto("c1", "m1", "alice",
                Photo.stored("chat/c1/m1.jpg", 1600, 1200), Instant.parse("2026-10-13T20:00:00Z")));
    }

    @Test
    void toDomain_shouldMapNullToNull() {
        assertThat(mapper.toDomain(null)).isNull();
    }
}
