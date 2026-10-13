package com.calendar.chat.domain.models;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConversationDetailTest {

    private static ConversationDetail event(String organizerId, List<String> members) {
        ConversationDetail conversation = new ConversationDetail("c1", members, ConversationType.EVENT, List.of(), 0,
                "Soirée jeux", "e1");
        conversation.setOrganizerId(organizerId);
        return conversation;
    }

    @Test
    void moderatorId_shouldBeTheNamedOrganizer() {
        assertThat(event("olivia", List.of("alice", "olivia")).moderatorId()).isEqualTo("olivia");
    }

    @Test
    @DisplayName("a group announced before the organizer was named falls back on its first member")
    void moderatorId_shouldFallBackOnTheFirstMember() {
        assertThat(event(null, List.of("olivia", "alice")).moderatorId()).isEqualTo("olivia");
        assertThat(event(null, List.of()).moderatorId()).isNull();
    }

    @Test
    void moderatorId_shouldBeNoOne_inAPrivateConversation() {
        ConversationDetail direct = new ConversationDetail("c1", List.of("alice", "bob"), ConversationType.DIRECT,
                List.of(), 0, null, null);

        assertThat(direct.moderatorId()).isNull();
    }

    @Test
    void mayRemovePhotoOf_shouldLetTheSenderAndTheOrganizer_andNoOneElse() {
        ConversationDetail group = event("olivia", List.of("olivia", "alice", "bob"));

        assertThat(group.mayRemovePhotoOf("alice", "alice")).isTrue();
        assertThat(group.mayRemovePhotoOf("alice", "olivia")).isTrue();
        assertThat(group.mayRemovePhotoOf("alice", "bob")).isFalse();
    }

    @Test
    void mayRemovePhotoOf_shouldOnlyLetTheSender_inAPrivateConversation() {
        ConversationDetail direct = new ConversationDetail("c1", List.of("alice", "bob"), ConversationType.DIRECT,
                List.of(), 0, null, null);

        assertThat(direct.mayRemovePhotoOf("alice", "alice")).isTrue();
        assertThat(direct.mayRemovePhotoOf("alice", "bob")).isFalse();
    }
}
