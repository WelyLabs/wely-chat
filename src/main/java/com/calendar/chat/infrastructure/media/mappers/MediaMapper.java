package com.calendar.chat.infrastructure.media.mappers;

import com.calendar.chat.domain.models.StoredMedia;
import com.calendar.chat.infrastructure.media.models.StoredMediaResponse;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

/** Translates wely-media's answers into the domain. */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface MediaMapper {

    StoredMedia toStoredMedia(StoredMediaResponse response);
}
