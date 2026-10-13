package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** Translates between the domain message and the broker contract. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MessageBroadcastMapper {

    /** The message's fields and its kind side by side: the record stays flat on the topic. */
    @Mapping(target = ".", source = "message")
    MessageBroadcastDTO toBroadcastDTO(LiveMessage live);

    /** A record from a pod that predates updates carries no kind: it was a new message. */
    @Mapping(target = "message", source = "dto")
    @Mapping(target = "kind", source = "kind", defaultValue = "NEW")
    LiveMessage toLiveMessage(MessageBroadcastDTO dto);

    Message toMessage(MessageBroadcastDTO dto);

    MessageBroadcastDTO.PhotoDTO toPhotoDTO(Photo photo);

    /** The URL is signed by the pod that delivers the message, not sent along. */
    @Mapping(target = "url", ignore = true)
    Photo toPhoto(MessageBroadcastDTO.PhotoDTO dto);
}
