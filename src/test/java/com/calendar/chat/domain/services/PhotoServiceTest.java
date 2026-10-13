package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.NormalizedPhoto;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.ports.PhotoNormalizer;
import com.calendar.chat.domain.ports.PhotoStorage;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PhotoServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-13T20:00:00Z");
    private static final byte[] UPLOAD = {1, 2, 3};
    private static final NormalizedPhoto NORMALIZED = new NormalizedPhoto(new byte[] {9}, "image/jpeg", 1600, 1200);

    @Mock private ChatRepository chatRepository;
    @Mock private ConversationPhotoRepository photoRepository;
    @Mock private PhotoStorage storage;
    @Mock private PhotoNormalizer normalizer;
    @Mock private ChatService chatService;

    private PhotoService service;

    /** Olivia organizes, Alice and Bob take part. */
    private final ConversationDetail eventGroup = group(ConversationType.EVENT);

    private static ConversationDetail group(ConversationType type) {
        ConversationDetail conversation = new ConversationDetail("c1", List.of("olivia", "alice", "bob"), type,
                List.of(), 0, "Soirée jeux", "e1");
        conversation.setOrganizerId(type == ConversationType.EVENT ? "olivia" : null);
        return conversation;
    }

    @BeforeEach
    void setUp() {
        service = new PhotoService(chatRepository, photoRepository, storage, normalizer, chatService,
                Clock.fixed(NOW, ZoneOffset.UTC));
        lenient().when(storage.signedUrl(anyString(), any())).thenAnswer(call -> "https://media/" + call.getArgument(0));
    }

    private void member(ConversationDetail conversation, String userId) {
        when(chatRepository.findById("c1", userId)).thenReturn(Mono.just(conversation));
    }

    private static ConversationPhoto photoOf(String senderId) {
        return new ConversationPhoto("c1", "m1", senderId, Photo.stored("chat/c1/m1.jpg", 1600, 1200), NOW);
    }

    @Test
    @DisplayName("a photo is stored, then sent as a message, then listed, and comes back with a signed URL")
    void send_shouldStoreThenSendThenList() {
        member(eventGroup, "alice");
        when(normalizer.normalize(UPLOAD)).thenReturn(Mono.just(NORMALIZED));
        when(storage.put(anyString(), eq(NORMALIZED.content()), eq("image/jpeg"))).thenReturn(Mono.empty());
        when(chatService.sendMessage(any())).thenAnswer(call -> Mono.just(call.getArgument(0)));
        when(photoRepository.save(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, "  Le podium ! "))
                .assertNext(sent -> {
                    assertThat(sent.id()).isNotBlank();
                    assertThat(sent.content()).isEqualTo("Le podium !");
                    assertThat(sent.timestamp()).isEqualTo(NOW);
                    assertThat(sent.photo().key()).isEqualTo("chat/c1/" + sent.id() + ".jpg");
                    assertThat(sent.photo().width()).isEqualTo(1600);
                    assertThat(sent.photo().url()).isEqualTo("https://media/chat/c1/" + sent.id() + ".jpg");
                })
                .verifyComplete();

        ArgumentCaptor<ConversationPhoto> listed = ArgumentCaptor.forClass(ConversationPhoto.class);
        verify(photoRepository).save(listed.capture());
        assertThat(listed.getValue().senderId()).isEqualTo("alice");
        // What is listed is the key, never a URL that would expire.
        assertThat(listed.getValue().photo().url()).isNull();
    }

    @Test
    void send_shouldRefuseSomeoneOutsideTheConversation_beforeDecodingAnything() {
        when(chatRepository.findById("c1", "mallory")).thenReturn(Mono.empty());

        StepVerifier.create(service.send("c1", "mallory", "Mallory", UPLOAD, ""))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.CONVERSATION_NOT_FOUND)
                .verify();
        verify(normalizer, never()).normalize(any());
        verify(storage, never()).put(any(), any(), any());
    }

    @Test
    @DisplayName("if the message cannot be written, the stored file is removed again")
    void send_shouldRemoveTheFile_whenTheMessageFails() {
        member(eventGroup, "alice");
        when(normalizer.normalize(UPLOAD)).thenReturn(Mono.just(NORMALIZED));
        when(storage.put(anyString(), any(), any())).thenReturn(Mono.empty());
        when(chatService.sendMessage(any())).thenReturn(Mono.error(new IllegalStateException("mongo down")));
        when(storage.delete(anyString())).thenReturn(Mono.empty());

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, ""))
                .expectError(IllegalStateException.class)
                .verify();
        verify(storage).delete(startsWith("chat/c1/"));
        verify(photoRepository, never()).save(any());
    }

    @Test
    void send_shouldStillSend_whenTheMediaTabEntryFails() {
        member(eventGroup, "alice");
        when(normalizer.normalize(UPLOAD)).thenReturn(Mono.just(NORMALIZED));
        when(storage.put(anyString(), any(), any())).thenReturn(Mono.empty());
        when(chatService.sendMessage(any())).thenAnswer(call -> Mono.just(call.getArgument(0)));
        when(photoRepository.save(any())).thenReturn(Mono.error(new IllegalStateException("index down")));

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, "")).expectNextCount(1).verifyComplete();
    }

    @Test
    void send_shouldNotStoreAnything_whenThePhotoIsRefused() {
        member(eventGroup, "alice");
        when(normalizer.normalize(UPLOAD)).thenReturn(Mono.error(new ChatException(ChatErrorCode.PHOTO_INVALID)));

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, ""))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.PHOTO_INVALID)
                .verify();
        verify(storage, never()).put(any(), any(), any());
    }

    @Test
    @DisplayName("someone who joined later pages through the photos sent before, with signed URLs")
    void page_shouldListTheConversationsPhotos_toAnyCurrentMember() {
        member(eventGroup, "bob");
        when(photoRepository.findPage("c1", null, 30)).thenReturn(Flux.just(photoOf("alice")));

        StepVerifier.create(service.page("c1", "bob", null, 30))
                .assertNext(photo -> assertThat(photo.photo().url()).isEqualTo("https://media/chat/c1/m1.jpg"))
                .verifyComplete();
    }

    @Test
    void page_shouldClampTheLimit() {
        member(eventGroup, "bob");
        when(photoRepository.findPage(eq("c1"), any(), eq(PhotoService.MAX_PAGE))).thenReturn(Flux.empty());

        StepVerifier.create(service.page("c1", "bob", null, 5000)).verifyComplete();
    }

    @Test
    void page_shouldAnswerNotFound_toSomeoneOutsideTheConversation() {
        when(chatRepository.findById("c1", "mallory")).thenReturn(Mono.empty());

        StepVerifier.create(service.page("c1", "mallory", null, 30))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.CONVERSATION_NOT_FOUND)
                .verify();
        verify(photoRepository, never()).findPage(any(), any(), any(Integer.class));
    }

    @Test
    void remove_shouldLetTheSenderRemoveTheirPhoto_everywhere() {
        member(eventGroup, "alice");
        when(photoRepository.find("c1", "m1")).thenReturn(Mono.just(photoOf("alice")));
        when(chatRepository.markPhotoRemoved("c1", "m1")).thenReturn(Mono.empty());
        when(photoRepository.delete("c1", "m1")).thenReturn(Mono.empty());
        when(storage.delete("chat/c1/m1.jpg")).thenReturn(Mono.empty());

        StepVerifier.create(service.remove("c1", "m1", "alice")).verifyComplete();

        verify(chatRepository).markPhotoRemoved("c1", "m1");
        verify(photoRepository).delete("c1", "m1");
        verify(storage).delete("chat/c1/m1.jpg");
    }

    @Test
    void remove_shouldLetTheOrganizerRemoveSomeoneElsesPhoto_inAnEventGroup() {
        member(eventGroup, "olivia");
        when(photoRepository.find("c1", "m1")).thenReturn(Mono.just(photoOf("alice")));
        when(chatRepository.markPhotoRemoved("c1", "m1")).thenReturn(Mono.empty());
        when(photoRepository.delete("c1", "m1")).thenReturn(Mono.empty());
        when(storage.delete(anyString())).thenReturn(Mono.empty());

        StepVerifier.create(service.remove("c1", "m1", "olivia")).verifyComplete();
    }

    @Test
    void remove_shouldRejectWhenRequesterIsNeitherSenderNorOrganizer() {
        member(eventGroup, "bob");
        when(photoRepository.find("c1", "m1")).thenReturn(Mono.just(photoOf("alice")));

        StepVerifier.create(service.remove("c1", "m1", "bob"))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.PHOTO_REMOVAL_DENIED)
                .verify();
        verify(chatRepository, never()).markPhotoRemoved(any(), any());
        verify(storage, never()).delete(any());
    }

    @Test
    void remove_shouldRejectAnotherMember_inAPrivateConversation() {
        member(group(ConversationType.DIRECT), "bob");
        when(photoRepository.find("c1", "m1")).thenReturn(Mono.just(photoOf("alice")));

        StepVerifier.create(service.remove("c1", "m1", "bob"))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.PHOTO_REMOVAL_DENIED)
                .verify();
    }

    @Test
    void remove_shouldAnswerNotFound_forAPhotoThatIsNotThere() {
        member(eventGroup, "alice");
        when(photoRepository.find("c1", "gone")).thenReturn(Mono.empty());

        StepVerifier.create(service.remove("c1", "gone", "alice"))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.PHOTO_NOT_FOUND)
                .verify();
    }

    @Test
    @DisplayName("a file that cannot be deleted leaves an orphan, not a failed removal")
    void remove_shouldSucceed_whenOnlyTheFileDeletionFails() {
        member(eventGroup, "alice");
        when(photoRepository.find("c1", "m1")).thenReturn(Mono.just(photoOf("alice")));
        when(chatRepository.markPhotoRemoved("c1", "m1")).thenReturn(Mono.empty());
        when(photoRepository.delete("c1", "m1")).thenReturn(Mono.empty());
        when(storage.delete(anyString())).thenReturn(Mono.error(new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE)));

        StepVerifier.create(service.remove("c1", "m1", "alice")).verifyComplete();
    }

    @Test
    void signed_shouldSignAStoredPhoto_andLeaveTextAndRemovedPhotosAlone() {
        Message text = new Message("m0", "alice", "Alice", null, "c1", "hi", NOW);
        Message removed = new Message("m2", "alice", "Alice", null, "c1", "", NOW, Photo.REMOVED);
        Message stored = new Message("m1", "alice", "Alice", null, "c1", "", NOW, Photo.stored("chat/c1/m1.jpg", 10, 10));

        assertThat(service.signed(text)).isSameAs(text);
        assertThat(service.signed(removed)).isSameAs(removed);
        assertThat(service.signed(stored).photo().url()).isEqualTo("https://media/chat/c1/m1.jpg");
    }

    @Test
    void signed_shouldSignEveryMessageOfABucketAndOfAConversation() {
        Message stored = new Message("m1", "alice", "Alice", null, "c1", "", NOW, Photo.stored("chat/c1/m1.jpg", 10, 10));
        ConversationDetail conversation = group(ConversationType.EVENT);
        conversation.setMessages(List.of(stored));

        assertThat(service.signed(new MessageBucket("c1", 0, List.of(stored))).messages().getFirst().photo().url())
                .isEqualTo("https://media/chat/c1/m1.jpg");
        assertThat(service.signed(conversation).getMessages().getFirst().photo().url())
                .isEqualTo("https://media/chat/c1/m1.jpg");
    }

}
