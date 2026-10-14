package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.infrastructure.messaging.models.CallEventBroadcastDTO;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/** Call events to and from the broker. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface CallEventBroadcastMapper {

    CallEventBroadcastDTO toBroadcastDTO(CallEvent event);

    CallEvent toCallEvent(CallEventBroadcastDTO dto);

    CallEventBroadcastDTO.CallDTO toDTO(Call call);

    Call toCall(CallEventBroadcastDTO.CallDTO dto);

    CallEventBroadcastDTO.ParticipantDTO toDTO(CallParticipant participant);

    CallParticipant toParticipant(CallEventBroadcastDTO.ParticipantDTO dto);
}
