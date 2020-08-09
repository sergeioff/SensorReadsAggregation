package com.pogorelovs.sensor.first;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class SparkSqlTest {
    protected static SparkSession sparkSession;

    @BeforeAll
    protected static void init() {
        sparkSession = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }

    @AfterAll
    protected static void releaseResources() {
        sparkSession.stop();
    }
}
