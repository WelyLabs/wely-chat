package com.calendar.chat.infrastructure.persistence.mappers;

import com.calendar.chat.domain.models.Call;
import com.calendar.chat.domain.models.CallParticipant;
import com.calendar.chat.infrastructure.persistence.models.entities.CallEntity;
import com.calendar.chat.infrastructure.persistence.models.entities.CallParticipantEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** A call as stored; {@code live} is derived from the status, for the unique index. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface CallPersistenceMapper {

    @Mapping(target = "live", expression = "java(call.isLive())")
    CallEntity toEntity(Call call);

    Call toDomain(CallEntity entity);

    CallParticipantEntity toEntity(CallParticipant participant);

    CallParticipant toDomain(CallParticipantEntity entity);
}
