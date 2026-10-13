package com.calendar.chat.application.mappers;

import com.calendar.chat.application.dtos.LiveMessageDTO;
import com.calendar.chat.domain.models.LiveMessage;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

/** Flattens a live message into the frame the browser reads. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface LiveMessageMapper {

    @Mapping(target = ".", source = "message")
    LiveMessageDTO toDTO(LiveMessage live);
}
