package com.calendar.chat.infrastructure.users.mappers;

import com.calendar.chat.domain.models.DeletedUser;
import com.calendar.chat.domain.models.DeletedUsersPage;
import com.calendar.chat.infrastructure.users.models.DeletedUserResponse;
import com.calendar.chat.infrastructure.users.models.DeletedUsersPageResponse;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DeletedUsersMapperTest {

    private static final Instant DELETED_AT = Instant.parse("2026-10-15T14:03:27.123Z");
    private static final UUID ALICE = UUID.fromString("89a380b8-0000-4000-8000-000000000001");
    private static final UUID BOB = UUID.fromString("89a380b8-0000-4000-8000-000000000002");

    private final DeletedUsersMapper mapper = Mappers.getMapper(DeletedUsersMapper.class);

    @Test
    void toPage_shouldMapTheItemsAndTheNextCursor() {
        DeletedUsersPageResponse response = new DeletedUsersPageResponse(
                List.of(new DeletedUserResponse(ALICE, DELETED_AT), new DeletedUserResponse(BOB, null)), "c1");

        assertThat(mapper.toPage(response)).isEqualTo(new DeletedUsersPage(
                List.of(new DeletedUser(ALICE.toString(), DELETED_AT), new DeletedUser(BOB.toString(), null)), "c1"));
    }

    @Test
    void toPage_shouldMapMissingItemsToAnEmptyPage() {
        assertThat(mapper.toPage(new DeletedUsersPageResponse(null, null))).isEqualTo(new DeletedUsersPage(List.of(), null));
    }

    @Test
    void toPage_shouldMapNullToNull() {
        assertThat(mapper.toPage(null)).isNull();
    }

    @Test
    void toDeletedUser_shouldKeepTheUserIdAndDeletedAt() {
        assertThat(mapper.toDeletedUser(new DeletedUserResponse(ALICE, DELETED_AT)))
                .isEqualTo(new DeletedUser("89a380b8-0000-4000-8000-000000000001", DELETED_AT));
    }

    @Test
    void toDeletedUser_shouldWriteTheUserIdInItsCanonicalLowercaseForm() {
        UUID uppercase = UUID.fromString("3F0C2A4E-8B1D-4C6A-9E2F-1A2B3C4D5E6F");

        assertThat(mapper.toDeletedUser(new DeletedUserResponse(uppercase, DELETED_AT)))
                .isEqualTo(new DeletedUser("3f0c2a4e-8b1d-4c6a-9e2f-1a2b3c4d5e6f", DELETED_AT));
    }

    @Test
    void toDeletedUser_shouldMapNullToNull() {
        assertThat(mapper.toDeletedUser(null)).isNull();
    }
}
