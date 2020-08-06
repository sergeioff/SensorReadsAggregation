package com.pogorelovs.sensor.enrichment;

import java.sql.Timestamp;
import java.time.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TimestampGenerator {

    /**
     * Generates time slots (timestamps) with specified interval between two dates
     * @param interval desired duration for one time slot
     * @param startDate start date (inclusive)
     * @param endDate end date (inclusive)
     * @return generated time slots (start time of each time slot)
     */
    public List<Timestamp> generateTimeStamps(Duration interval, LocalDate startDate, LocalDate endDate) {

        final var minutesBetweenDates = Period.between(startDate, endDate.plusDays(1)).getDays() * 24 * 60;
        final var minutesPerTimeSlot = interval.toMinutes();

        final LocalDateTime startDateTime = LocalDateTime.of(startDate, LocalTime.MIDNIGHT);

        final long numberOfTimeSlots = minutesBetweenDates / minutesPerTimeSlot;

        return LongStream.range(0, numberOfTimeSlots)
                .mapToObj(timeSlotMultiplier -> Timestamp.valueOf(startDateTime.plusMinutes(timeSlotMultiplier * minutesPerTimeSlot)))
                .collect(Collectors.toList());
    }
}
