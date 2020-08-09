package com.pogorelovs.sensor.first.aggregation;

import com.pogorelovs.sensor.first.constant.DataOutputConstants;
import com.pogorelovs.sensor.first.structure.MetadataStructure;
import com.pogorelovs.sensor.first.structure.ValuesStructure;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.spark.sql.functions.col;

public class AggregationTests {

    private final String VALUES_FILE_PATH = "/values.csv";
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
    public void testAggregation() {
        final var valuesDataset = sparkSession.read()
                .schema(ValuesStructure.STRUCTURE)
                .csv(this.getClass().getResource(VALUES_FILE_PATH).getPath());

        final var metaDataset = sparkSession.read()
                .schema(MetadataStructure.STRUCTURE)
                .csv(this.getClass().getResource(META_FILE_PATH).getPath());

        final var aggregatedDataset = SensorReadsAggregation.aggregateData(
                metaDataset, valuesDataset, Duration.ofMinutes(15)
        );

        final var sortedResultRows = aggregatedDataset.sort(col(DataOutputConstants.COL_TIME_SLOT_START))
                .collectAsList();

        Assertions.assertEquals(2, sortedResultRows.size());

        final var firstWindow = sortedResultRows.get(0);
        Assertions.assertEquals(61.01, firstWindow.<Double>getAs(DataOutputConstants.COL_TEMP_MIN));
        Assertions.assertEquals(65.01, firstWindow.<Double>getAs(DataOutputConstants.COL_TEMP_MAX));
        Assertions.assertEquals(63.26, firstWindow.<Double>getAs(DataOutputConstants.COL_TEMP_AVG));
        Assertions.assertEquals(4L, firstWindow.<Long>getAs(DataOutputConstants.COL_TEMP_COUNT));
        Assertions.assertEquals(true, firstWindow.<Boolean>getAs(DataOutputConstants.COL_PRESENCE));
        Assertions.assertEquals(2L, firstWindow.<Long>getAs(DataOutputConstants.COL_PRESENCE_COUNT));

        final var secondWindow = sortedResultRows.get(1);
        Assertions.assertEquals(50.01, secondWindow.<Double>getAs(DataOutputConstants.COL_TEMP_MIN));
        Assertions.assertEquals(50.01, secondWindow.<Double>getAs(DataOutputConstants.COL_TEMP_MAX));
        Assertions.assertEquals(50.01, secondWindow.<Double>getAs(DataOutputConstants.COL_TEMP_AVG));
        Assertions.assertEquals(1L, secondWindow.<Long>getAs(DataOutputConstants.COL_TEMP_COUNT));
        Assertions.assertEquals(false, secondWindow.<Boolean>getAs(DataOutputConstants.COL_PRESENCE));
        Assertions.assertEquals(0, secondWindow.<Long>getAs(DataOutputConstants.COL_PRESENCE_COUNT));
    }
}
