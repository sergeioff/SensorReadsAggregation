package com.pogorelovs.sensor.generator;

import java.sql.Timestamp;
import java.time.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TimestampGenerator {

    /**
     * Generates time slots (timestamps) with specified interval between two dates
     *
     * @param interval      desired duration for one time slot
     * @param startDateTime start date and time (inclusive)
     * @param endDateTime   end date and time (inclusive)
     * @return generated time slots (start time of each time slot)
     */
    public List<Timestamp> generateTimestamps(Duration interval, LocalDateTime startDateTime, LocalDateTime endDateTime) {
        final var minutesBetweenDates = Duration.between(startDateTime, endDateTime.plusDays(1)).toMinutes();
        final var minutesPerTimeSlot = interval.toMinutes();

        final long numberOfTimeSlots = minutesBetweenDates / minutesPerTimeSlot;

        return LongStream.range(0, numberOfTimeSlots)
                .mapToObj(timeSlotMultiplier -> Timestamp.valueOf(startDateTime.plusMinutes(timeSlotMultiplier * minutesPerTimeSlot)))
                .collect(Collectors.toList());
    }

//    public Dataset<Row> getTimestampsDataSet(Duration interval, LocalDateTime startDateime, LocalDateTime endDateTime, SparkSession sparkSession) {
//        final var timestamps = this.generateTimestamps(interval, startDate, endDate);
//        final var timestampRows = timestamps.stream().map(RowFactory::create).collect(Collectors.toList());
//
//        return sparkSession.createDataFrame(timestampRows, DataTypes.createStructType(List.of(
//                DataTypes.createStructField(DataOutputConstants.COL_TIME_SLOT_START, DataTypes.TimestampType, false)
//        )));
//    }
}
