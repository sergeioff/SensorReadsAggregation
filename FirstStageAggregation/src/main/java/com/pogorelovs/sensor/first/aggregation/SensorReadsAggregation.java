package com.pogorelovs.sensor.first.aggregation;

import com.pogorelovs.sensor.first.constant.DataOutputConstants;
import com.pogorelovs.sensor.first.constant.DataSourceConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.time.Duration;
import java.util.List;

import static com.pogorelovs.sensor.first.utils.Utils.seq;
import static org.apache.spark.sql.functions.*;

public class SensorReadsAggregation {

    private static final String WINDOW_DURATION_FORMAT = "%d minutes";

    public static Dataset<Row> aggregateData(Dataset<Row> meta, Dataset<Row> values, Duration timeSlotDuration) {
        final var joinedDataset = values.join(meta,
                seq(List.of(DataSourceConstants.COL_SENSOR_ID, DataSourceConstants.COL_CHANNEL_ID))
        );

        joinedDataset.cache();

        final var groupingExpression = seq(List.of(
                date_format(window(
                        col(DataSourceConstants.COL_TIMESTAMP),
                        String.format(WINDOW_DURATION_FORMAT, timeSlotDuration.toMinutes())
                        ).apply("start"), DataOutputConstants.TIMESTAMP_FORMAT
                ).as(DataOutputConstants.COL_TIME_SLOT_START),
                col(DataSourceConstants.COL_LOCATION_ID)
        ));


        final var temperatures = joinedDataset
                .filter(col(DataSourceConstants.COL_CHANNEL_TYPE).eqNullSafe(DataSourceConstants.SENSOR_TEMPERATURE))
                .groupBy(groupingExpression)
                .agg(
                        bround(min(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_MIN),
                        bround(max(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_MAX),
                        bround(avg(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_AVG),
                        bround(count(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_COUNT)
                );

        final var presences = joinedDataset
                .filter(col(DataSourceConstants.COL_CHANNEL_TYPE).eqNullSafe(DataSourceConstants.SENSOR_PRESENCE))
                .groupBy(groupingExpression)
                .agg(
                        sum(DataSourceConstants.COL_VALUE).cast(DataTypes.BooleanType)
                                .as(DataOutputConstants.COL_PRESENCE),
                        sum(DataSourceConstants.COL_VALUE).cast(DataTypes.LongType)
                                .as(DataOutputConstants.COL_PRESENCE_COUNT)
                );

        joinedDataset.unpersist();

        return temperatures.join(presences, seq(List.of(
                DataOutputConstants.COL_TIME_SLOT_START, DataSourceConstants.COL_LOCATION_ID))
        );
    }
}
