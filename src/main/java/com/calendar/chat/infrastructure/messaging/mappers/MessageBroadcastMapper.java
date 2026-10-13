package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.Message;
import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import com.calendar.chat.domain.models.Photo;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

/** Translates between the domain message and the broker contract. */
@Mapper(componentModel = "spring")
public interface MessageBroadcastMapper {

    MessageBroadcastDTO toBroadcastDTO(Message message);

    Message toMessage(MessageBroadcastDTO dto);

    /** The URL is signed by the pod that delivers the message, not sent along. */
    @Mapping(target = "url", ignore = true)
    Photo toPhoto(MessageBroadcastDTO.PhotoDTO dto);
}
