package com.calendar.chat.application.rest;

import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.services.PhotoService;
import com.calendar.chat.exception.BusinessErrorCode;
import com.calendar.chat.exception.BusinessException;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.FormFieldPart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * A conversation's photos: send one (a message carrying it), page through them for the media tab,
 * remove one. Members only; who is asking comes from the token.
 */
@RestController
@RequestMapping("/conversations/{conversationId}/photos")
public class PhotoController {

    private static final String BUSINESS_ID = "businessId";
    /** Above this, refused before decoding. The web app sends photos already scaled down. */
    static final int MAX_PHOTO_BYTES = 10 * 1024 * 1024;
    static final int MAX_CAPTION_LENGTH = 2000;

    private final PhotoService photoService;

    public PhotoController(PhotoService photoService) {
        this.photoService = photoService;
    }

    /**
     * Multipart: {@code file}, and an optional {@code caption}. 413 CHT-BUS-005 above 10 MB, 400
     * CHT-BUS-004 for a file that is not an accepted image. Answers with the message as sent.
     */
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Message> send(@AuthenticationPrincipal Jwt jwt, @PathVariable String conversationId,
                              @RequestBody Mono<MultiValueMap<String, Part>> form) {
        String senderId = jwt.getClaimAsString(BUSINESS_ID);
        String senderName = jwt.getClaimAsString("preferred_username");

        return form.flatMap(parts -> {
            if (!(parts.getFirst("file") instanceof FilePart file)) {
                return Mono.error(new BusinessException(BusinessErrorCode.PHOTO_INVALID));
            }
            String caption = parts.getFirst("caption") instanceof FormFieldPart field ? field.value() : "";
            if (caption.length() > MAX_CAPTION_LENGTH) {
                return Mono.error(new BusinessException(BusinessErrorCode.PHOTO_INVALID));
            }
            return DataBufferUtils.join(file.content(), MAX_PHOTO_BYTES)
                    .map(buffer -> {
                        byte[] bytes = new byte[buffer.readableByteCount()];
                        buffer.read(bytes);
                        DataBufferUtils.release(buffer);
                        return bytes;
                    })
                    .onErrorMap(DataBufferLimitException.class, e -> new BusinessException(BusinessErrorCode.PHOTO_TOO_LARGE))
                    .switchIfEmpty(Mono.error(new BusinessException(BusinessErrorCode.PHOTO_INVALID)))
                    .flatMap(bytes -> photoService.send(conversationId, senderId, senderName, bytes, caption));
        });
    }

    /** Newest first; {@code before} is the {@code sentAt} of the last photo already shown. */
    @GetMapping
    public Flux<ConversationPhoto> page(@AuthenticationPrincipal Jwt jwt, @PathVariable String conversationId,
                                        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant before,
                                        @RequestParam(defaultValue = "30") int limit) {
        return photoService.page(conversationId, jwt.getClaimAsString(BUSINESS_ID), before, limit);
    }

    /** By its sender, or in an event's group by its organizer: 403 CHT-BUS-007 otherwise. */
    @DeleteMapping("/{messageId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> remove(@AuthenticationPrincipal Jwt jwt, @PathVariable String conversationId,
                             @PathVariable String messageId) {
        return photoService.remove(conversationId, messageId, jwt.getClaimAsString(BUSINESS_ID));
    }
}
