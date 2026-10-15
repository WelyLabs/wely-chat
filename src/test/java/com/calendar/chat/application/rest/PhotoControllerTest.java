package com.calendar.chat.application.rest;

import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.services.PhotoService;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.FormFieldPart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PhotoControllerTest {

    @Mock private PhotoService photoService;
    @Mock private Jwt jwt;

    private PhotoController controller;

    @BeforeEach
    void setUp() {
        controller = new PhotoController(photoService);
        lenient().when(jwt.getClaimAsString("businessId")).thenReturn("alice");
        lenient().when(jwt.getClaimAsString("preferred_username")).thenReturn("alice");
    }

    private static FilePart file(int size) {
        FilePart file = mock(FilePart.class);
        DataBuffer buffer = DefaultDataBufferFactory.sharedInstance.wrap(new byte[size]);
        when(file.content()).thenReturn(Flux.just(buffer));
        return file;
    }

    private static Mono<MultiValueMap<String, Part>> form(Part file, String caption) {
        MultiValueMap<String, Part> parts = new LinkedMultiValueMap<>();
        if (file != null) {
            parts.add("file", file);
        }
        if (caption != null) {
            FormFieldPart field = mock(FormFieldPart.class);
            lenient().when(field.value()).thenReturn(caption);
            parts.add("caption", field);
        }
        return Mono.just(parts);
    }

    private static boolean failsWith(Throwable error, ChatErrorCode code) {
        return error instanceof ChatException chat && chat.getErrorCode() == code;
    }

    @Test
    void send_shouldHandTheBytesAndCaptionToTheService_asTheCaller() {
        Message sent = new Message("m1", "alice", "alice", null, "c1", "Podium", Instant.EPOCH,
                Photo.stored("chat/c1/m1.jpg", 10, 10));
        when(photoService.send(eq("c1"), eq("alice"), eq("alice"), any(), eq("Podium"))).thenReturn(Mono.just(sent));

        StepVerifier.create(controller.send(jwt, "c1", form(file(3), "Podium"))).expectNext(sent).verifyComplete();
    }

    @Test
    void send_shouldFailWithAccountDeleted_whenTheSenderIsDeleted() {
        when(photoService.send(eq("c1"), eq("alice"), eq("alice"), any(), eq("")))
                .thenReturn(Mono.error(new ChatException(ChatErrorCode.ACCOUNT_DELETED)));

        StepVerifier.create(controller.send(jwt, "c1", form(file(3), null)))
                .expectErrorMatches(error -> failsWith(error, ChatErrorCode.ACCOUNT_DELETED))
                .verify();
    }

    @Test
    void send_shouldFailWithReadOnly_whenTheConversationIsReadOnly() {
        when(photoService.send(eq("c1"), eq("alice"), eq("alice"), any(), eq("")))
                .thenReturn(Mono.error(new ChatException(ChatErrorCode.CONVERSATION_READ_ONLY)));

        StepVerifier.create(controller.send(jwt, "c1", form(file(3), null)))
                .expectErrorMatches(error -> failsWith(error, ChatErrorCode.CONVERSATION_READ_ONLY))
                .verify();
    }

    @Test
    void send_shouldSendWithoutCaption_whenThereIsNone() {
        when(photoService.send(eq("c1"), eq("alice"), eq("alice"), any(), eq(""))).thenReturn(Mono.empty());

        StepVerifier.create(controller.send(jwt, "c1", form(file(3), null))).verifyComplete();
    }

    @Test
    void send_shouldRefuseAPhotoOverTenMegabytes_withoutCallingTheService() {
        StepVerifier.create(controller.send(jwt, "c1", form(file(PhotoController.MAX_PHOTO_BYTES + 1), null)))
                .expectErrorMatches(error -> failsWith(error, ChatErrorCode.PHOTO_TOO_LARGE))
                .verify();
        verify(photoService, never()).send(any(), any(), any(), any(), any());
    }

    @Test
    void send_shouldRefuseAFormWithoutAFile() {
        StepVerifier.create(controller.send(jwt, "c1", form(null, "just text")))
                .expectErrorMatches(error -> failsWith(error, ChatErrorCode.PHOTO_INVALID))
                .verify();
    }

    @Test
    void send_shouldRefuseAnOverlongCaption() {
        StepVerifier.create(controller.send(jwt, "c1", form(mock(FilePart.class), "x".repeat(PhotoController.MAX_CAPTION_LENGTH + 1))))
                .expectErrorMatches(error -> failsWith(error, ChatErrorCode.PHOTO_INVALID))
                .verify();
    }

    @Test
    void page_shouldAskForTheCallersView() {
        ConversationPhoto photo = new ConversationPhoto("c1", "m1", "bob", Photo.stored("k", 1, 1), Instant.EPOCH);
        Instant before = Instant.parse("2026-10-13T20:00:00Z");
        when(photoService.page("c1", "alice", before, 30)).thenReturn(Flux.just(photo));

        StepVerifier.create(controller.page(jwt, "c1", before, 30)).expectNext(photo).verifyComplete();
    }

    @Test
    void remove_shouldRemoveAsTheCaller() {
        when(photoService.remove("c1", "m1", "alice")).thenReturn(Mono.empty());

        StepVerifier.create(controller.remove(jwt, "c1", "m1")).verifyComplete();

        verify(photoService).remove("c1", "m1", "alice");
    }
}
