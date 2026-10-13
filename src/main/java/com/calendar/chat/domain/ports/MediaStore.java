package com.calendar.chat.domain.ports;

import com.calendar.chat.domain.models.MediaProfile;
import com.calendar.chat.domain.models.StoredMedia;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

/** Where conversation photos are kept, privately: nothing stored is readable without a signed URL. */
public interface MediaStore {

    /** Prepares and stores an upload under {@code key}. Fails with PHOTO_INVALID, PHOTO_TOO_LARGE or MEDIA_UNAVAILABLE. */
    Mono<StoredMedia> store(String key, byte[] content, MediaProfile profile);

    /** Removing a missing key is not an error. */
    Mono<Void> delete(String key);

    /** Short-lived signed URLs, by key; a key the store does not know is left out. */
    Mono<Map<String, String>> urls(Collection<String> keys);
}
