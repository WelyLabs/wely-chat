package com.calendar.chat.infrastructure.messaging.mappers;

import com.calendar.chat.domain.models.EventGroup;
import com.calendar.chat.infrastructure.messaging.models.EventGroupChangedDTO;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface EventGroupMapper {

    EventGroup toEventGroup(EventGroupChangedDTO dto);
}
