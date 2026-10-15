package com.calendar.chat.infrastructure.persistence.adapters;

import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.infrastructure.persistence.mappers.ConversationPhotoMapper;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationPhotoEntity;
import com.calendar.chat.infrastructure.persistence.repositories.ConversationPhotoMongoRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Component
public class MongoConversationPhotoRepositoryAdapter implements ConversationPhotoRepository {

    private final ConversationPhotoMongoRepository repository;
    private final ConversationPhotoMapper mapper;

    public MongoConversationPhotoRepositoryAdapter(ConversationPhotoMongoRepository repository, ConversationPhotoMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /** Saving the same message's photo twice keeps one entry. */
    @Override
    public Mono<Void> save(ConversationPhoto photo) {
        return repository.save(mapper.toEntity(photo))
                .onErrorResume(DuplicateKeyException.class, already -> Mono.empty())
                .then();
    }

    @Override
    public Flux<ConversationPhoto> findPage(String conversationId, Instant before, int limit) {
        Flux<ConversationPhotoEntity> page = before == null
                ? repository.findByConversationIdOrderBySentAtDesc(conversationId, Limit.of(limit))
                : repository.findByConversationIdAndSentAtBeforeOrderBySentAtDesc(conversationId, before, Limit.of(limit));
        return page.map(mapper::toDomain);
    }

    @Override
    public Mono<ConversationPhoto> find(String conversationId, String messageId) {
        return repository.findByConversationIdAndMessageId(conversationId, messageId).map(mapper::toDomain);
    }

    @Override
    public Mono<Void> delete(String conversationId, String messageId) {
        return repository.deleteByConversationIdAndMessageId(conversationId, messageId).then();
    }

    @Override
    public Flux<ConversationPhoto> findAll(String conversationId) {
        return repository.findByConversationId(conversationId).map(mapper::toDomain);
    }

    @Override
    public Mono<Void> deleteAll(String conversationId) {
        return repository.deleteByConversationId(conversationId).then();
    }
}
