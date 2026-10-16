package com.calendar.chat.domain.models;

/** What one reconciliation pass did: tombstones read, erasures applied, erasures that failed. */
public record ReconciliationReport(long tombstones, long erased, long failed) {

    public static final ReconciliationReport NONE = new ReconciliationReport(0, 0, 0);

    public ReconciliationReport plus(ReconciliationReport other) {
        return new ReconciliationReport(tombstones + other.tombstones, erased + other.erased, failed + other.failed);
    }
}
