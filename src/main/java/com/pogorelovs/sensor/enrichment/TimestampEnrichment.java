package com.pogorelovs.sensor.enrichment;

import com.pogorelovs.sensor.utils.Utils;
import com.pogorelovs.sensor.constant.DataOutputConstants;
import com.pogorelovs.sensor.constant.DataSourceConstants;
import com.pogorelovs.sensor.generator.TimestampGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class TimestampEnrichment {

    static Dataset<Row> generateTimestampsPerRoomsDataset(SparkSession spark, Dataset<Row> meta, List<Timestamp> timestamps) {

        final var timestampRows = timestamps.stream().map(RowFactory::create).collect(Collectors.toList());

        final var timestampsDataset = spark.createDataFrame(timestampRows,
                DataTypes.createStructType(List.of(
                        DataTypes.createStructField(DataOutputConstants.COL_TIME_SLOT_START, DataTypes.TimestampType, false)
                )));

        return meta.select(col(DataSourceConstants.COL_LOCATION_ID))
                .distinct()
                .join(
                        timestampsDataset.select(date_format(
                                col(DataOutputConstants.COL_TIME_SLOT_START), DataOutputConstants.TIMESTAMP_FORMAT
                        ).as(DataOutputConstants.COL_TIME_SLOT_START))
                );
    }

    /**
     * Enriches resulting dataset (after aggregation) with missing timeslots
     *
     * @param sparkSession     spark sql session
     * @param resultingDataset resulting dataset
     * @param metaDataset      dataset with metadata
     * @param startDate        start date
     * @param endDate          end date
     * @param timeslotDuration duration of one time slot
     * @return enriched dataset (combined with missing time slots)
     */
    public static Dataset<Row> enrichResultingDataset(SparkSession sparkSession,
                                                      Dataset<Row> resultingDataset, Dataset<Row> metaDataset,
                                                      LocalDate startDate, LocalDate endDate,
                                                      Duration timeslotDuration) {

        final var timestampsList = TimestampGenerator.generateTimestamps(
                timeslotDuration,
                LocalDateTime.of(startDate, LocalTime.MIDNIGHT),
                LocalDateTime.of(endDate, LocalTime.MIDNIGHT)
        );

        final var timestampsPerRoomsDataset = generateTimestampsPerRoomsDataset(sparkSession, metaDataset, timestampsList);

        final var nullValueReplacements = Map.<String, Object>of(
                DataOutputConstants.COL_PRESENCE, false,
                DataOutputConstants.COL_TEMP_COUNT, 0,
                DataOutputConstants.COL_PRESENCE_COUNT, 0
        );

        return resultingDataset.join(timestampsPerRoomsDataset,
                Utils.seq(List.of(DataOutputConstants.COL_TIME_SLOT_START, DataSourceConstants.COL_LOCATION_ID)),
                "outer")
                .na()
                .fill(nullValueReplacements);
    }
}
