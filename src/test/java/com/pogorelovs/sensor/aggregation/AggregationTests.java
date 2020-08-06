package com.pogorelovs.sensor.aggregation;

import com.pogorelovs.sensor.structure.MetadataStructure;
import com.pogorelovs.sensor.structure.ValuesStructure;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AggregationTests {

    private final String VALUES_FILE_PATH = "/values.csv";
    private final String META_FILE_PATH = "/meta.csv";

    private SparkSession sparkSession;

    @BeforeEach
    private void init() {
        sparkSession = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }

    @AfterEach
    private void releaseResources() {
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

        valuesDataset.show();
        metaDataset.show();

        final var aggregatedDataset = SensorReadsAggregation.aggregateData(metaDataset, valuesDataset);

        aggregatedDataset.show();
    }
}
