package com.pogorelovs.sensor.enrichment;

import com.pogorelovs.sensor.utils.Utils;
import com.pogorelovs.sensor.aggregation.SensorReadsAggregation;
import com.pogorelovs.sensor.constant.DataOutputConstants;
import com.pogorelovs.sensor.constant.DataSourceConstants;
import com.pogorelovs.sensor.generator.TimestampGenerator;
import com.pogorelovs.sensor.structure.MetadataStructure;
import com.pogorelovs.sensor.structure.ValuesStructure;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class TimestampEnrichmentTests {

    private final String META_FILE_PATH = "/meta.csv";
    private final String VALUES_FILE_PATH = "/values.csv";

    private static SparkSession sparkSession;

    @BeforeAll
    private static void init() {
        sparkSession = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    private static void releaseResources() {
        sparkSession.stop();
    }

    @Test
    public void testTimestampsForRoomsGeneration() {
        final var meta = sparkSession.read()
                .schema(MetadataStructure.STRUCTURE)
                .csv(
                        this.getClass().getResource(META_FILE_PATH).getPath()
                );


        final var timestamps = TimestampGenerator.generateTimestamps(
                Duration.ofMinutes(15),
                LocalDateTime.of(2018, 9, 1, 0, 0),
                LocalDateTime.of(2018, 9, 3, 0, 0)
        );

        final var timestampsForRooms = TimestampEnrichment.generateTimestampsPerRoomsDataset(
                sparkSession, meta, timestamps
        );

        Assertions.assertEquals(288 * 2, timestampsForRooms.count());  // there are 2 rooms in the test meta file

    }

    @Test
    public void testEnrichment() {
        final var valuesDataset = sparkSession.read()
                .schema(ValuesStructure.STRUCTURE)
                .csv(this.getClass().getResource(VALUES_FILE_PATH).getPath());

        final var metaDataset = sparkSession.read()
                .schema(MetadataStructure.STRUCTURE)
                .csv(this.getClass().getResource(META_FILE_PATH).getPath());

        final var timeslotDuration = Duration.ofMinutes(15);

        final var aggregatedDataset = SensorReadsAggregation.aggregateData(
                metaDataset, valuesDataset, timeslotDuration
        );

        final var enrichedDataset = TimestampEnrichment.enrichResultingDataset(
                sparkSession, aggregatedDataset, metaDataset,
                LocalDate.of(2020, 7, 5),
                LocalDate.of(2020, 7, 5),
                timeslotDuration
        );

        final var sortedResult = enrichedDataset.sort(Utils.seq(List.of(
                col(DataOutputConstants.COL_TIME_SLOT_START),
                col(DataSourceConstants.COL_LOCATION_ID)
        ))).collectAsList();

        Assertions.assertEquals(2 * 24 * 4, sortedResult.size()); // 2 rooms, 24 hours, 4 slots per hour

        final var row1 = sortedResult.get(0);
        final var row2 = sortedResult.get(1);
        final var row3 = sortedResult.get(2);
        final var row4 = sortedResult.get(3);

        Assertions.assertEquals("Room 0", row1.<String>getAs(DataSourceConstants.COL_LOCATION_ID));
        Assertions.assertEquals("Room 2", row2.<String>getAs(DataSourceConstants.COL_LOCATION_ID));
        Assertions.assertEquals("Room 0", row3.<String>getAs(DataSourceConstants.COL_LOCATION_ID));
        Assertions.assertEquals("Room 2", row4.<String>getAs(DataSourceConstants.COL_LOCATION_ID));

        Assertions.assertNotNull(row1.<Double>getAs(DataOutputConstants.COL_TEMP_AVG));
        Assertions.assertNull(row2.<Double>getAs(DataOutputConstants.COL_TEMP_AVG));
        Assertions.assertNotNull(row3.<Double>getAs(DataOutputConstants.COL_TEMP_AVG));
        Assertions.assertNull(row4.<Double>getAs(DataOutputConstants.COL_TEMP_AVG));

        Assertions.assertTrue(row1.<Boolean>getAs(DataOutputConstants.COL_PRESENCE));
        Assertions.assertFalse(row2.<Boolean>getAs(DataOutputConstants.COL_PRESENCE));
        Assertions.assertFalse(row3.<Boolean>getAs(DataOutputConstants.COL_PRESENCE));
        Assertions.assertFalse(row4.<Boolean>getAs(DataOutputConstants.COL_PRESENCE));
    }
}
