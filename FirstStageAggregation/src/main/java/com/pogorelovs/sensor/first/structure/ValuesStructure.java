package com.pogorelovs.sensor.first.structure;

import com.pogorelovs.sensor.first.constant.DataSourceConstants;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public interface ValuesStructure {
    StructType STRUCTURE = DataTypes.createStructType(List.of(
            DataTypes.createStructField(DataSourceConstants.COL_SENSOR_ID, DataTypes.StringType, false),
            DataTypes.createStructField(DataSourceConstants.COL_CHANNEL_ID, DataTypes.StringType, false),
            DataTypes.createStructField(DataSourceConstants.COL_TIMESTAMP, DataTypes.TimestampType, false),
            DataTypes.createStructField(DataSourceConstants.COL_VALUE, DataTypes.DoubleType, false)
    ));
}
