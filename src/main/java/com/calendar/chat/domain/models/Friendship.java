package com.calendar.chat.domain.models;

/**
 * Two users who are friends, as wely-social last announced them; {@code userA} is always the lower id.
 *
 * @param version rises with every change of the pair, so a stale or replayed record is dropped
 */
public record Friendship(String userA, String userB, long version) {

    private static final String SEPARATOR = ":";

    public Friendship {
        if (userA == null || userB == null || userA.equals(userB)) {
            throw new IllegalArgumentException("A friendship needs two distinct users");
        }
        if (userA.compareTo(userB) > 0) {
            String lower = userB;
            userB = userA;
            userA = lower;
        }
    }

    public String pairId() {
        return pairIdOf(userA, userB);
    }

    /** The pair's key, the same whichever side is named first: the one wely-social's records carry. */
    public static String pairIdOf(String userId, String otherId) {
        return userId.compareTo(otherId) <= 0 ? userId + SEPARATOR + otherId : otherId + SEPARATOR + userId;
    }
}
