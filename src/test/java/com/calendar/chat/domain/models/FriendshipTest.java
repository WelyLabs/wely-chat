package com.calendar.chat.domain.models;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FriendshipTest {

    private static final String LOW = "11111111-0000-4000-8000-000000000001";
    private static final String HIGH = "22222222-0000-4000-8000-000000000002";

    @Test
    void constructor_shouldPutTheLowerIdFirst_whicheverSideIsNamedFirst() {
        Friendship friendship = new Friendship(HIGH, LOW, 3);

        assertThat(friendship.userA()).isEqualTo(LOW);
        assertThat(friendship.userB()).isEqualTo(HIGH);
        assertThat(friendship.version()).isEqualTo(3);
    }

    @Test
    void constructor_shouldRefuseOneUserTwice() {
        assertThatThrownBy(() -> new Friendship(LOW, LOW, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructor_shouldRefuseAMissingUser() {
        assertThatThrownBy(() -> new Friendship(null, LOW, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pairId_shouldJoinTheTwoIdsInOrder() {
        assertThat(new Friendship(HIGH, LOW, 1).pairId()).isEqualTo(LOW + ":" + HIGH);
    }

    @Test
    void pairIdOf_shouldBeTheSame_whicheverSideIsNamedFirst() {
        assertThat(Friendship.pairIdOf(HIGH, LOW)).isEqualTo(Friendship.pairIdOf(LOW, HIGH)).isEqualTo(LOW + ":" + HIGH);
    }
}
