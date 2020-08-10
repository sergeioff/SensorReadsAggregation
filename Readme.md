# Sensor read aggregation
Spark jobs to aggregate sensor reads data from [datasets](datasets)

## Merging results
In order to merge aggregation results use:
```
hadoop fs -getmerge {output_dir} {resulting_file}
```

Example: ```hadoop fs -getmerge out out.csv```

note: it's possible to achieve the same behavior via `dataframe.coalesce(1)` but it's not recommended.

## Build
```
gradle package
```

## Test run
```
gradle clean package
java -jar build/FirstStageAggregation-develop-all.jar -from 2018-03-23 -until 2018-03-23 -duration 15 -meta datasets/meta.csv -values datasets/values.csv -out firstStage
hadoop fs -getmerge firstStage firstStage.csv
java -jar build/SecondStageAggregation-develop-all.jar -in firstStage.csv -out secondStage.json
```