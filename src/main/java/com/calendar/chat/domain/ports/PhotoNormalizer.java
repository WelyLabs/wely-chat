package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.NormalizedPhoto;
import reactor.core.publisher.Mono;

/** Turns an upload into what is stored: a bounded JPEG without metadata. Fails with PHOTO_INVALID. */
public interface PhotoNormalizer {

    Mono<NormalizedPhoto> normalize(byte[] upload);
}
