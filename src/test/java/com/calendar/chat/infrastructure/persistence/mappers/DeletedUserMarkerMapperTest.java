package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.DeletedUserMarker;
import com.calendar.chat.infrastructure.persistence.models.entities.DeletedUserMarkerEntity;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DeletedUserMarkerMapperTest {

    private static final Instant DELETED_AT = Instant.parse("2026-10-15T14:03:27.123Z");
    private static final Instant ERASED_AT = Instant.parse("2026-10-15T14:03:29Z");

    private final DeletedUserMarkerMapper mapper = new DeletedUserMarkerMapperImpl();

    @Test
    void toDomain_shouldMapTheIdToUserId() {
        assertThat(mapper.toDomain(new DeletedUserMarkerEntity("alice", DELETED_AT, ERASED_AT)))
                .isEqualTo(new DeletedUserMarker("alice", DELETED_AT, ERASED_AT));
        assertThat(mapper.toDomain(null)).isNull();
    }

    @Test
    void toEntity_shouldMapUserIdToTheId() {
        assertThat(mapper.toEntity(new DeletedUserMarker("alice", DELETED_AT, null)))
                .usingRecursiveComparison()
                .isEqualTo(new DeletedUserMarkerEntity("alice", DELETED_AT, null));
        assertThat(mapper.toEntity(null)).isNull();
    }
}
