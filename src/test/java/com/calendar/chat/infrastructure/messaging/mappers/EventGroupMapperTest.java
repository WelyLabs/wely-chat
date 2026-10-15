package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.infrastructure.messaging.models.EventGroupChangedDTO;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventGroupMapperTest {

    private final EventGroupMapper mapper = Mappers.getMapper(EventGroupMapper.class);

    @Test
    void toEventGroup_shouldCarryEveryField() {
        EventGroupChangedDTO dto = new EventGroupChangedDTO("event-1", "Board games", "ines", List.of("ines", "me"), 4, false);

        assertThat(mapper.toEventGroup(dto)).isEqualTo(new EventGroup("event-1", "Board games", "ines", List.of("ines", "me"), 4));
    }

    @Test
    void toEventGroup_shouldCarryTheDeletion() {
        EventGroupChangedDTO dto = new EventGroupChangedDTO("event-1", "Board games", null, List.of(), 5, true);

        assertThat(mapper.toEventGroup(dto).deleted()).isTrue();
    }

    @Test
    void toEventGroup_shouldMapNullToNull() {
        assertThat(mapper.toEventGroup(null)).isNull();
    }
}
