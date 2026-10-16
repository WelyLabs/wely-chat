package com.calendar.chat.domain.models;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReconciliationReportTest {

    @Test
    void plus_shouldAddEachCount() {
        ReconciliationReport sum = new ReconciliationReport(500, 3, 1).plus(new ReconciliationReport(20, 2, 0));

        assertThat(sum).isEqualTo(new ReconciliationReport(520, 5, 1));
    }

    @Test
    void plus_shouldChangeNothing_whenAddingTheEmptyReport() {
        ReconciliationReport report = new ReconciliationReport(7, 1, 2);

        assertThat(report.plus(ReconciliationReport.NONE)).isEqualTo(report);
        assertThat(ReconciliationReport.NONE.plus(report)).isEqualTo(report);
    }
}
