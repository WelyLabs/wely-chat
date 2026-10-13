package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.infrastructure.persistence.models.entities.ConversationPhotoEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** The media tab's entry: flat in MongoDB, a {@link Photo} in the domain. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface ConversationPhotoMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "key", source = "photo.key")
    @Mapping(target = "width", source = "photo.width")
    @Mapping(target = "height", source = "photo.height")
    ConversationPhotoEntity toEntity(ConversationPhoto photo);

    default ConversationPhoto toDomain(ConversationPhotoEntity entity) {
        return entity == null ? null : new ConversationPhoto(entity.getConversationId(), entity.getMessageId(),
                entity.getSenderId(), Photo.stored(entity.getKey(), entity.getWidth(), entity.getHeight()), entity.getSentAt());
    }
}
