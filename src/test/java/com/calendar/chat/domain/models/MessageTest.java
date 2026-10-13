package com.calendar.chat.domain.models;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class MessageTest {

    private static Message carrying(Photo photo) {
        return new Message("m1", "alice", "Alice", null, "c1", "", Instant.EPOCH, photo);
    }

    @Test
    void constructor_shouldMakeATextMessage_withoutAPhoto() {
        Message text = new Message("m1", "alice", "Alice", null, "c1", "hi", Instant.EPOCH);

        assertThat(text.photo()).isNull();
        assertThat(text.hasStoredPhoto()).isFalse();
    }

    @Test
    void hasStoredPhoto_shouldBeTrueOnlyForAPhotoStillInStorage() {
        assertThat(carrying(Photo.stored("chat/c1/m1.jpg", 1600, 1200)).hasStoredPhoto()).isTrue();
        assertThat(carrying(Photo.REMOVED).hasStoredPhoto()).isFalse();
    }
}
