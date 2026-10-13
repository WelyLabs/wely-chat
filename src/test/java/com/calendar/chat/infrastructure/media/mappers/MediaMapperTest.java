package com.calendar.chat.infrastructure.media.mappers;

import com.calendar.chat.domain.models.StoredMedia;
import com.calendar.chat.infrastructure.media.models.StoredMediaResponse;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import static org.assertj.core.api.Assertions.assertThat;

class MediaMapperTest {

    private final MediaMapper mapper = Mappers.getMapper(MediaMapper.class);

    @Test
    void toStoredMedia_shouldCarryTheKeyAndSize() {
        StoredMediaResponse response = new StoredMediaResponse("chat/c1/m1.jpg", 1600, 1200, "image/jpeg");

        assertThat(mapper.toStoredMedia(response)).isEqualTo(new StoredMedia("chat/c1/m1.jpg", 1600, 1200));
    }

    @Test
    void toStoredMedia_shouldMapNullToNull() {
        assertThat(mapper.toStoredMedia(null)).isNull();
    }
}
