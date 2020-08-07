package com.pogorelovs.sensor.enrichment;

import com.pogorelovs.sensor.generator.TimestampGenerator;
import com.pogorelovs.sensor.structure.MetadataStructure;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;

public class TimestampEnrichmentTests {

    private final String META_FILE_PATH = "/meta.csv";

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

        final var timestampsForRooms = TimestampEnrichment.generateTimestampsPerRoomsDataset(sparkSession, meta, timestamps);

        Assertions.assertEquals(288 * 2, timestampsForRooms.count());  // there are 2 rooms in the test meta file

    }
}
