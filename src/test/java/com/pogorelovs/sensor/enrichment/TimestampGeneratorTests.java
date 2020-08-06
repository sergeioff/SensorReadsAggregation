package com.pogorelovs.sensor.enrichment;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class TimestampGeneratorTests {

    private TimestampGenerator timeStampGenerator = new TimestampGenerator();

    @Test
    public void testTimeStampGeneration() {
        final var timestamps = timeStampGenerator.generateTimeStamps(
                Duration.ofMinutes(15),
                LocalDate.of(2018, 9, 1),
                LocalDate.of(2018, 9, 3)
        );

        Assertions.assertEquals(288, timestamps.size());

        Assertions.assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2018, 9, 1, 0, 0)),
                timestamps.get(0)
        );

        Assertions.assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2018, 9, 1, 0, 15)),
                timestamps.get(1)
        );

        Assertions.assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2018, 9, 3, 23, 45)),
                timestamps.get(timestamps.size() - 1)
        );
    }
}