package com.calendar.chat.application.mappers;

import com.calendar.chat.application.dtos.CallDTO;
import com.calendar.chat.application.dtos.CallEventDTO;
import com.calendar.chat.application.dtos.CallJoinResponse;
import com.calendar.chat.application.dtos.LayersRequest;
import com.calendar.chat.application.dtos.PublishRequest;
import com.calendar.chat.application.dtos.SessionDescriptionDTO;
import com.calendar.chat.application.dtos.SubscribeRequest;
import com.calendar.chat.application.dtos.SubscriptionResponse;
import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallEvent;
import com.calendar.chat.domain.models.CallJoin;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.domain.models.CallSubscription;
import com.calendar.chat.domain.models.IceServer;
import com.calendar.chat.domain.models.LayerChoice;
import com.calendar.chat.domain.models.LocalTrack;
import com.calendar.chat.domain.models.SessionDescription;
import com.calendar.chat.domain.models.TrackRequest;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/** Calls between HTTP or RSocket and the domain. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface CallRestMapper {

    CallDTO toDTO(Call call);

    CallDTO.Participant toDTO(CallParticipant participant);

    CallEventDTO toDTO(CallEvent event);

    CallJoinResponse toResponse(CallJoin join);

    CallJoinResponse.IceServerDTO toDTO(IceServer server);

    SessionDescriptionDTO toDTO(SessionDescription description);

    SessionDescription toDomain(SessionDescriptionDTO description);

    LocalTrack toDomain(PublishRequest.Track track);

    TrackRequest toDomain(SubscribeRequest.Track track);

    LayerChoice toDomain(LayersRequest.Choice choice);

    SubscriptionResponse toResponse(CallSubscription subscription);

    SubscriptionResponse.Track toDTO(CallSubscription.ReceivedTrack track);
}
