package com.pogorelovs.sensor.aggregation;

import com.pogorelovs.sensor.constant.DataOutputConstants;
import com.pogorelovs.sensor.constant.DataSourceConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.immutable.Set;

import static org.apache.spark.sql.functions.*;

public class SensorReadsAggregation {

    private static final String WINDOW_DURATION = "15 minutes";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    public static Dataset<Row> aggregateData(Dataset<Row> meta, Dataset<Row> values) {
        final var joinedDataset = values.join(meta,
                new Set.Set2<>(DataSourceConstants.COL_SENSOR_ID, DataSourceConstants.COL_CHANNEL_ID).toSeq()
        );

        joinedDataset.cache();

        final var groupingExpression = new Set.Set2<>(
                date_format(window(
                        col(DataSourceConstants.COL_TIMESTAMP), WINDOW_DURATION).apply("start"), TIMESTAMP_FORMAT
                ).as(DataOutputConstants.COL_TIME_SLOT_START),
                col(DataSourceConstants.COL_LOCATION_ID)
        ).toSeq();


        final var temperatures = joinedDataset
                .filter(col(DataSourceConstants.COL_CHANNEL_TYPE).eqNullSafe(DataSourceConstants.SENSOR_TEMPERATURE))
                .groupBy(groupingExpression)
                .agg(
                        bround(functions.min(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_MIN),
                        bround(functions.max(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_MAX),
                        bround(functions.avg(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_AVG),
                        bround(functions.count(DataSourceConstants.COL_VALUE), 2).as(DataOutputConstants.COL_TEMP_COUNT)
                );

        final var presences = joinedDataset
                .filter(col(DataSourceConstants.COL_CHANNEL_TYPE).eqNullSafe(DataSourceConstants.SENSOR_PRESENCE))
                .groupBy(groupingExpression)
                .agg(
                        functions.sum(DataSourceConstants.COL_VALUE).cast(DataTypes.BooleanType).as(DataOutputConstants.COL_PRESENCE),
                        count(when(col(DataSourceConstants.COL_VALUE).gt(0), 1)).as(DataOutputConstants.COL_PRESENCE_COUNT)
                );

        return temperatures.join(presences,
                new Set.Set2<>(DataOutputConstants.COL_TIME_SLOT_START, DataSourceConstants.COL_LOCATION_ID).toSeq()
        );
    }
}
