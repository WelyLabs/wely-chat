package com.calendar.chat.domain.services;

import com.calendar.chat.domain.models.BroadcastKind;
import com.calendar.chat.domain.models.ConversationDetail;
import com.calendar.chat.domain.models.ConversationPhoto;
import com.calendar.chat.domain.models.ConversationType;
import com.calendar.chat.domain.models.Message;
import com.calendar.chat.domain.models.MessageBucket;
import com.calendar.chat.domain.models.LiveMessage;
import com.calendar.chat.domain.models.MediaProfile;
import com.calendar.chat.domain.models.Photo;
import com.calendar.chat.domain.models.StoredMedia;
import com.calendar.chat.domain.ports.ChatRepository;
import com.calendar.chat.domain.ports.ConversationPhotoRepository;
import com.calendar.chat.domain.ports.MediaStore;
import com.calendar.chat.exception.ChatErrorCode;
import com.calendar.chat.exception.ChatException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PhotoServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-13T20:00:00Z");
    private static final byte[] UPLOAD = {1, 2, 3};

    @Mock private ChatRepository chatRepository;
    @Mock private ConversationPhotoRepository photoRepository;
    @Mock private MediaStore mediaStore;
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
        service = new PhotoService(chatRepository, photoRepository, mediaStore, chatService,
                Clock.fixed(NOW, ZoneOffset.UTC));
        lenient().when(mediaStore.urls(any())).thenAnswer(call -> Mono.just(signedUrls(call.getArgument(0))));
        lenient().when(chatRepository.findMessage(anyString(), anyString())).thenReturn(Mono.empty());
    }

    private static Map<String, String> signedUrls(Collection<String> keys) {
        // Null while a test re-stubs the call.
        return keys == null ? Map.of() : keys.stream().collect(Collectors.toMap(key -> key, key -> "https://media/" + key));
    }

    /** wely-media keeps the key it was given and reports the prepared size. */
    private void storesAs(int width, int height) {
        when(mediaStore.store(anyString(), eq(UPLOAD), eq(MediaProfile.PHOTO)))
                .thenAnswer(call -> Mono.just(new StoredMedia(call.getArgument(0), width, height)));
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
        storesAs(1600, 1200);
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
        verify(mediaStore, never()).store(any(), any(), any());
    }

    @Test
    @DisplayName("if the message cannot be written, the stored file is removed again")
    void send_shouldRemoveTheFile_whenTheMessageFails() {
        member(eventGroup, "alice");
        storesAs(1600, 1200);
        when(chatService.sendMessage(any())).thenReturn(Mono.error(new IllegalStateException("mongo down")));
        when(mediaStore.delete(anyString())).thenReturn(Mono.empty());

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, ""))
                .expectError(IllegalStateException.class)
                .verify();
        verify(mediaStore).delete(startsWith("chat/c1/"));
        verify(photoRepository, never()).save(any());
    }

    @Test
    void send_shouldStillSend_whenTheMediaTabEntryFails() {
        member(eventGroup, "alice");
        storesAs(1600, 1200);
        when(chatService.sendMessage(any())).thenAnswer(call -> Mono.just(call.getArgument(0)));
        when(photoRepository.save(any())).thenReturn(Mono.error(new IllegalStateException("index down")));

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, "")).expectNextCount(1).verifyComplete();
    }

    @Test
    void send_shouldNotSendTheMessage_whenThePhotoIsRefused() {
        member(eventGroup, "alice");
        when(mediaStore.store(anyString(), any(), any())).thenReturn(Mono.error(new ChatException(ChatErrorCode.PHOTO_INVALID)));

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, ""))
                .expectErrorMatches(error -> error instanceof ChatException chat
                        && chat.getErrorCode() == ChatErrorCode.PHOTO_INVALID)
                .verify();
        verify(chatService, never()).sendMessage(any());
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
        when(mediaStore.delete("chat/c1/m1.jpg")).thenReturn(Mono.empty());

        StepVerifier.create(service.remove("c1", "m1", "alice")).verifyComplete();

        verify(chatRepository).markPhotoRemoved("c1", "m1");
        verify(photoRepository).delete("c1", "m1");
        verify(mediaStore).delete("chat/c1/m1.jpg");
    }

    @Test
    void remove_shouldLetTheOrganizerRemoveSomeoneElsesPhoto_inAnEventGroup() {
        member(eventGroup, "olivia");
        when(photoRepository.find("c1", "m1")).thenReturn(Mono.just(photoOf("alice")));
        when(chatRepository.markPhotoRemoved("c1", "m1")).thenReturn(Mono.empty());
        when(photoRepository.delete("c1", "m1")).thenReturn(Mono.empty());
        when(mediaStore.delete(anyString())).thenReturn(Mono.empty());

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
        verify(mediaStore, never()).delete(any());
        verify(chatService, never()).broadcastUpdate(any(), any());
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
        when(mediaStore.delete(anyString())).thenReturn(Mono.error(new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE)));

        StepVerifier.create(service.remove("c1", "m1", "alice")).verifyComplete();
    }

    private void removable(String senderId) {
        when(photoRepository.find("c1", "m1")).thenReturn(Mono.just(photoOf(senderId)));
        when(chatRepository.markPhotoRemoved("c1", "m1")).thenReturn(Mono.empty());
        when(photoRepository.delete("c1", "m1")).thenReturn(Mono.empty());
        when(mediaStore.delete(anyString())).thenReturn(Mono.empty());
        // As read back from its bucket: no conversation id, which the mapper leaves out.
        when(chatRepository.findMessage("c1", "m1")).thenReturn(Mono.just(
                new Message("m1", senderId, "Alice", null, null, "Le podium", NOW, Photo.REMOVED)));
    }

    @Test
    @DisplayName("the other members see the photo go without reloading: the message is pushed again, removed")
    void remove_shouldPushTheUpdatedMessage_toTheOtherMembers() {
        member(eventGroup, "olivia");
        removable("alice");
        when(chatService.broadcastUpdate(any(), eq("olivia"))).thenReturn(Mono.empty());

        StepVerifier.create(service.remove("c1", "m1", "olivia")).verifyComplete();

        verify(chatService).broadcastUpdate(argThat(message -> message.id().equals("m1")
                && message.conversationId().equals("c1")
                && message.senderId().equals("alice")
                && message.content().equals("Le podium")
                && message.timestamp().equals(NOW)
                && Photo.REMOVED.equals(message.photo())), eq("olivia"));
    }

    @Test
    @DisplayName("the push comes after the message is marked and the file deleted")
    void remove_shouldPushAfterMarkingTheMessage() {
        member(eventGroup, "alice");
        removable("alice");
        when(chatService.broadcastUpdate(any(), any())).thenReturn(Mono.empty());

        StepVerifier.create(service.remove("c1", "m1", "alice")).verifyComplete();

        InOrder order = inOrder(chatRepository, mediaStore, chatService);
        order.verify(chatRepository).markPhotoRemoved("c1", "m1");
        order.verify(mediaStore).delete("chat/c1/m1.jpg");
        order.verify(chatRepository).findMessage("c1", "m1");
        order.verify(chatService).broadcastUpdate(any(), eq("alice"));
    }

    @Test
    @DisplayName("a push that fails costs the live update, not the removal")
    void remove_shouldSucceed_whenThePushFails() {
        member(eventGroup, "alice");
        removable("alice");
        when(chatService.broadcastUpdate(any(), any())).thenReturn(Mono.error(new IllegalStateException("broker down")));

        StepVerifier.create(service.remove("c1", "m1", "alice")).verifyComplete();
    }

    @Test
    void remove_shouldSucceed_whenTheMessageCannotBeReadBack() {
        member(eventGroup, "alice");
        removable("alice");
        when(chatRepository.findMessage("c1", "m1")).thenReturn(Mono.error(new IllegalStateException("mongo down")));

        StepVerifier.create(service.remove("c1", "m1", "alice")).verifyComplete();
        verify(chatService, never()).broadcastUpdate(any(), any());
    }

    @Test
    void signed_shouldSignALiveMessage_andKeepItsKind() {
        Message stored = new Message("m1", "alice", "Alice", "bob", "c1", "", NOW, Photo.stored("chat/c1/m1.jpg", 10, 10));

        StepVerifier.create(service.signed(new LiveMessage(stored, BroadcastKind.NEW)))
                .assertNext(live -> {
                    assertThat(live.kind()).isEqualTo(BroadcastKind.NEW);
                    assertThat(live.message().photo().url()).isEqualTo("https://media/chat/c1/m1.jpg");
                })
                .verifyComplete();
    }

    @Test
    void send_shouldReturnTheMessageWithoutUrl_whenSigningFails() {
        member(eventGroup, "alice");
        storesAs(1600, 1200);
        when(chatService.sendMessage(any())).thenAnswer(call -> Mono.just(call.getArgument(0)));
        when(photoRepository.save(any())).thenReturn(Mono.empty());
        when(mediaStore.urls(any())).thenReturn(Mono.error(new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE)));

        StepVerifier.create(service.send("c1", "alice", "Alice", UPLOAD, ""))
                .assertNext(sent -> {
                    assertThat(sent.photo().key()).startsWith("chat/c1/");
                    assertThat(sent.photo().url()).isNull();
                })
                .verifyComplete();
    }

    @Test
    void page_shouldSignTheWholePageInOneCall() {
        member(eventGroup, "bob");
        ConversationPhoto other = new ConversationPhoto("c1", "m2", "bob", Photo.stored("chat/c1/m2.jpg", 10, 10), NOW);
        when(photoRepository.findPage("c1", null, 30)).thenReturn(Flux.just(photoOf("alice"), other));

        StepVerifier.create(service.page("c1", "bob", null, 30)).expectNextCount(2).verifyComplete();

        verify(mediaStore, times(1)).urls(List.of("chat/c1/m1.jpg", "chat/c1/m2.jpg"));
    }

    @Test
    void page_shouldListPhotosWithoutUrl_whenSigningFails() {
        member(eventGroup, "bob");
        when(photoRepository.findPage("c1", null, 30)).thenReturn(Flux.just(photoOf("alice")));
        when(mediaStore.urls(any())).thenReturn(Mono.error(new ChatException(ChatErrorCode.MEDIA_UNAVAILABLE)));

        StepVerifier.create(service.page("c1", "bob", null, 30))
                .assertNext(photo -> {
                    assertThat(photo.photo().key()).isEqualTo("chat/c1/m1.jpg");
                    assertThat(photo.photo().url()).isNull();
                })
                .verifyComplete();
    }

    @Test
    void signed_shouldSignAStoredPhoto_andLeaveTextAndRemovedPhotosAlone() {
        Message text = new Message("m0", "alice", "Alice", null, "c1", "hi", NOW);
        Message removed = new Message("m2", "alice", "Alice", null, "c1", "", NOW, Photo.REMOVED);
        Message stored = new Message("m1", "alice", "Alice", null, "c1", "", NOW, Photo.stored("chat/c1/m1.jpg", 10, 10));

        StepVerifier.create(service.signed(text)).expectNext(text).verifyComplete();
        StepVerifier.create(service.signed(removed)).expectNext(removed).verifyComplete();
        StepVerifier.create(service.signed(stored))
                .assertNext(signed -> assertThat(signed.photo().url()).isEqualTo("https://media/chat/c1/m1.jpg"))
                .verifyComplete();
        // Nothing to sign, nothing asked of wely-media.
        verify(mediaStore, times(1)).urls(any());
    }

    @Test
    void signed_shouldReturnTheMessageWithoutUrl_whenSigningFails() {
        Message stored = new Message("m1", "alice", "Alice", null, "c1", "", NOW, Photo.stored("chat/c1/m1.jpg", 10, 10));
        when(mediaStore.urls(any())).thenReturn(Mono.error(new IllegalStateException("media down")));

        StepVerifier.create(service.signed(stored))
                .assertNext(signed -> {
                    assertThat(signed.photo().key()).isEqualTo("chat/c1/m1.jpg");
                    assertThat(signed.photo().url()).isNull();
                })
                .verifyComplete();
    }

    @Test
    void signed_shouldSignABucketInOneCall() {
        Message first = new Message("m1", "alice", "Alice", null, "c1", "", NOW, Photo.stored("chat/c1/m1.jpg", 10, 10));
        Message second = new Message("m2", "bob", "Bob", null, "c1", "", NOW, Photo.stored("chat/c1/m2.jpg", 10, 10));
        Message text = new Message("m3", "bob", "Bob", null, "c1", "hi", NOW);

        StepVerifier.create(service.signed(new MessageBucket("c1", 0, List.of(first, second, text))))
                .assertNext(bucket -> {
                    assertThat(bucket.conversationId()).isEqualTo("c1");
                    assertThat(bucket.messages()).extracting(message -> message.photo() == null ? null : message.photo().url())
                            .containsExactly("https://media/chat/c1/m1.jpg", "https://media/chat/c1/m2.jpg", null);
                })
                .verifyComplete();
        verify(mediaStore, times(1)).urls(List.of("chat/c1/m1.jpg", "chat/c1/m2.jpg"));
    }

    @Test
    void signed_shouldLeaveABucketWithoutMessagesAlone() {
        MessageBucket empty = new MessageBucket("c1", 0, null);

        StepVerifier.create(service.signed(empty)).expectNext(empty).verifyComplete();
        verify(mediaStore, never()).urls(any());
    }

    @Test
    void signed_shouldSignEveryMessageOfAConversation() {
        Message stored = new Message("m1", "alice", "Alice", null, "c1", "", NOW, Photo.stored("chat/c1/m1.jpg", 10, 10));
        ConversationDetail conversation = group(ConversationType.EVENT);
        conversation.setMessages(List.of(stored));

        StepVerifier.create(service.signed(conversation))
                .assertNext(signed -> assertThat(signed.getMessages().getFirst().photo().url())
                        .isEqualTo("https://media/chat/c1/m1.jpg"))
                .verifyComplete();
    }

    @Test
    @DisplayName("more than 200 photos are signed in calls of 200 keys at most")
    void signed_shouldChunkTheKeys_whenMoreThanTheMediaServiceAcceptsPerCall() {
        List<Message> messages = IntStream.range(0, 450)
                .mapToObj(i -> new Message("m" + i, "alice", "Alice", null, "c1", "", NOW,
                        Photo.stored("chat/c1/m" + i + ".jpg", 10, 10)))
                .toList();
        ConversationDetail conversation = group(ConversationType.EVENT);
        conversation.setMessages(messages);

        StepVerifier.create(service.signed(conversation))
                .assertNext(signed -> assertThat(signed.getMessages()).allMatch(message -> message.photo().url() != null))
                .verifyComplete();

        ArgumentCaptor<Collection<String>> calls = ArgumentCaptor.captor();
        verify(mediaStore, times(3)).urls(calls.capture());
        assertThat(calls.getAllValues()).extracting(Collection::size).containsExactly(200, 200, 50);
    }

    @Test
    @DisplayName("a chunk that fails costs its own URLs only")
    void signed_shouldKeepTheOtherChunksUrls_whenOneChunkFails() {
        List<Message> messages = IntStream.range(0, 250)
                .mapToObj(i -> new Message("m" + i, "alice", "Alice", null, "c1", "", NOW,
                        Photo.stored("chat/c1/m" + i + ".jpg", 10, 10)))
                .toList();
        when(mediaStore.urls(any()))
                .thenAnswer(call -> Mono.just(signedUrls(call.getArgument(0))))
                .thenReturn(Mono.error(new IllegalStateException("media down")));

        StepVerifier.create(service.signed(new MessageBucket("c1", 0, messages)))
                .assertNext(bucket -> {
                    assertThat(bucket.messages().subList(0, 200)).allMatch(message -> message.photo().url() != null);
                    assertThat(bucket.messages().subList(200, 250)).allMatch(message -> message.photo().url() == null);
                })
                .verifyComplete();
    }
}
