package com.pogorelovs.sensor.aggregation;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AggregationTests {

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
    public void testItWorks() {
        System.out.println(sparkSession.conf());
    }
}
