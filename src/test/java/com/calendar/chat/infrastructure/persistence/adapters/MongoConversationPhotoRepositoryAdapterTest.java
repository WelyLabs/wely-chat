package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.infrastructure.persistence.mappers.ConversationPhotoMapper;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationPhotoEntity;
import com.calendar.chat.infrastructure.persistence.repositories.ConversationPhotoMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mapstruct.factory.Mappers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Limit;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongoConversationPhotoRepositoryAdapterTest {

    private static final Instant SENT = Instant.parse("2026-10-13T20:00:00Z");

    @Mock private ConversationPhotoMongoRepository repository;

    private final ConversationPhotoMapper mapper = Mappers.getMapper(ConversationPhotoMapper.class);
    private MongoConversationPhotoRepositoryAdapter adapter;

    private final ConversationPhoto photo = new ConversationPhoto("c1", "m1", "alice", Photo.stored("k", 10, 10), SENT);
    private final ConversationPhotoEntity entity = new ConversationPhotoEntity("id", "c1", "m1", "alice", "k", 10, 10, SENT);

    @BeforeEach
    void setUp() {
        adapter = new MongoConversationPhotoRepositoryAdapter(repository, mapper);
    }

    @Test
    void save_shouldStoreTheEntry() {
        when(repository.save(any())).thenReturn(Mono.just(entity));

        StepVerifier.create(adapter.save(photo)).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void save_shouldKeepOneEntry_whenTheSamePhotoIsSavedTwice() {
        when(repository.save(any())).thenReturn(Mono.error(new DuplicateKeyException("conversation_message_unique")));

        StepVerifier.create(adapter.save(photo)).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void findPage_shouldStartFromTheNewest_withoutACursor() {
        when(repository.findByConversationIdOrderBySentAtDesc("c1", Limit.of(30))).thenReturn(Flux.just(entity));

        StepVerifier.create(adapter.findPage("c1", null, 30)).expectNext(photo).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void findPage_shouldContinueBeforeTheLastPhotoShown() {
        when(repository.findByConversationIdAndSentAtBeforeOrderBySentAtDesc("c1", SENT, Limit.of(30)))
                .thenReturn(Flux.empty());

        StepVerifier.create(adapter.findPage("c1", SENT, 30)).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void find_shouldNameTheConversation() {
        when(repository.findByConversationIdAndMessageId("c1", "m1")).thenReturn(Mono.just(entity));

        StepVerifier.create(adapter.find("c1", "m1")).expectNext(photo).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void delete_shouldNameTheConversation() {
        when(repository.deleteByConversationIdAndMessageId("c1", "m1")).thenReturn(Mono.just(1L));

        StepVerifier.create(adapter.delete("c1", "m1")).expectComplete().verify(Duration.ofSeconds(10));

        verify(repository).deleteByConversationIdAndMessageId("c1", "m1");
    }

    @Test
    void findAll_shouldReadEveryPhotoOfTheConversation() {
        when(repository.findByConversationId("c1")).thenReturn(Flux.just(entity));

        StepVerifier.create(adapter.findAll("c1")).expectNext(photo).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void deleteAll_shouldDeleteTheConversationsEntries() {
        when(repository.deleteByConversationId("c1")).thenReturn(Mono.just(2L));

        StepVerifier.create(adapter.deleteAll("c1")).expectComplete().verify(Duration.ofSeconds(10));

        verify(repository).deleteByConversationId("c1");
    }
}
