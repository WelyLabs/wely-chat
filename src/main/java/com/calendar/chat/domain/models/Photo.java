package com.calendar.chat.domain.models;

/**
 * A photo carried by a message.
 *
 * <p>{@code key} is where it lives in object storage; {@code url} is a short-lived signed address,
 * filled only when the message is handed to a client and never stored. A removed photo keeps its
 * place in the conversation, without a key, so the thread still shows that one was there.
 */
public record Photo(String key, int width, int height, String url, boolean removed) {

    public static Photo stored(String key, int width, int height) {
        return new Photo(key, width, height, null, false);
    }

    public static final Photo REMOVED = new Photo(null, 0, 0, null, true);

}
