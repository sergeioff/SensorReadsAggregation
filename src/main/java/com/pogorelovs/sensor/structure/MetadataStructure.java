package com.pogorelovs.sensor.structure;

import com.pogorelovs.sensor.constant.DataSourceConstants;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public interface MetadataStructure {
    StructType STRUCTURE = DataTypes.createStructType(List.of(
            DataTypes.createStructField(DataSourceConstants.COL_SENSOR_ID, DataTypes.StringType, false),
            DataTypes.createStructField(DataSourceConstants.COL_CHANNEL_ID, DataTypes.StringType, false),
            DataTypes.createStructField(DataSourceConstants.COL_CHANNEL_TYPE, DataTypes.StringType, false),
            DataTypes.createStructField(DataSourceConstants.COL_LOCATION_ID, DataTypes.StringType, false)
    ));
}
