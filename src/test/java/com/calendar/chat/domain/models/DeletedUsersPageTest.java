package com.calendar.chat.domain.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DeletedUsersPageTest {

    @Test
    void constructor_shouldReadMissingItemsAsAnEmptyPage() {
        assertThat(new DeletedUsersPage(null, null).items()).isEmpty();
    }

    @Test
    void constructor_shouldCopyTheItems_whenTheGivenListChangesLater() {
        List<DeletedUser> items = new ArrayList<>(List.of(new DeletedUser("alice", Instant.EPOCH)));

        DeletedUsersPage page = new DeletedUsersPage(items, "cursor");
        items.add(new DeletedUser("bob", Instant.EPOCH));

        assertThat(page.items()).extracting(DeletedUser::userId).containsExactly("alice");
        assertThat(page.next()).isEqualTo("cursor");
    }
}
