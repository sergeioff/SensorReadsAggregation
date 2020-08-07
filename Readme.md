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
gradle shadowJar
```