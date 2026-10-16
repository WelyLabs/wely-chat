package com.calendar.chat.domain.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class DeletedUserTest {

    @Test
    void constructor_shouldKeepTheAccountAndItsDeletionTime() {
        Instant deletedAt = Instant.parse("2026-10-15T14:03:27Z");

        DeletedUser user = new DeletedUser("alice", deletedAt);

        assertThat(user.userId()).isEqualTo("alice");
        assertThat(user.deletedAt()).isEqualTo(deletedAt);
    }

    @Test
    void constructor_shouldRefuseATombstoneWithoutUserId() {
        assertThatNullPointerException().isThrownBy(() -> new DeletedUser(null, Instant.EPOCH));
    }
}
