package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.Message;
import com.calendar.chat.infrastructure.messaging.models.MessageBroadcastDTO;
import org.mapstruct.Mapper;

/** Translates between the domain message and the broker contract. */
@Mapper(componentModel = "spring")
public interface MessageBroadcastMapper {

    MessageBroadcastDTO toBroadcastDTO(Message message);

    Message toMessage(MessageBroadcastDTO dto);
}
