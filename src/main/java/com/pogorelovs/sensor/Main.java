package com.pogorelovs.sensor;

import com.pogorelovs.sensor.aggregation.SensorReadsAggregation;
import com.pogorelovs.sensor.constant.CLIOptions;
import com.pogorelovs.sensor.constant.DataOutputConstants;
import com.pogorelovs.sensor.constant.DataSourceConstants;
import com.pogorelovs.sensor.enrichment.TimestampEnrichment;
import com.pogorelovs.sensor.structure.MetadataStructure;
import com.pogorelovs.sensor.structure.ValuesStructure;
import org.apache.commons.cli.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) throws ParseException {
        Options options = populateCliOptions();

        CommandLine providedArgs = new DefaultParser().parse(options, args);
        exitIfMissingOptions(providedArgs, options);

        final LocalDate from = LocalDate.parse(providedArgs.getOptionValue(CLIOptions.from));
        final LocalDate until = LocalDate.parse(providedArgs.getOptionValue(CLIOptions.until));
        final Duration timeslotDuration = Duration.ofMinutes(Integer.parseInt(providedArgs.getOptionValue(CLIOptions.duration)));
        final Path metadataPath = Paths.get(providedArgs.getOptionValue(CLIOptions.meta));
        final Path valuesPath = Paths.get(providedArgs.getOptionValue(CLIOptions.values));
        final Path outputPath = Paths.get(providedArgs.getOptionValue(CLIOptions.out));

        final var fromDateTime = LocalDateTime.of(from, LocalTime.MIDNIGHT);
        final var untilDateTime = LocalDateTime.of(until, LocalTime.MIDNIGHT);

        final SparkSession spark = SparkSession.builder()
                .master("local[*]") //FIXME: comment if you want to run aggregation as spark-submit job
                .appName("Sensor reads aggregation")
                .getOrCreate();

        final Dataset<Row> metadata = spark.read()
                .schema(MetadataStructure.STRUCTURE)
                .csv(metadataPath.toString());

        final Dataset<Row> values = spark.read()
                .schema(ValuesStructure.STRUCTURE)
                .csv(valuesPath.toString());

        final Dataset<Row> filteredValues = values.filter(col(DataSourceConstants.COL_TIMESTAMP)
                .between(Timestamp.valueOf(fromDateTime), Timestamp.valueOf(untilDateTime.plusDays(1))));

        final Dataset<Row> aggregatedData = SensorReadsAggregation.aggregateData(metadata, filteredValues, timeslotDuration);

        final Dataset<Row> enrichedDataset = TimestampEnrichment.enrichResultingDataset(
                spark, aggregatedData, metadata, from, until, timeslotDuration
        );

        enrichedDataset
                .sort(col(DataOutputConstants.COL_TIME_SLOT_START))
                .write()
                .option("nullValue", null)
                .csv(outputPath.toString());
    }

    private static Options populateCliOptions() {
        Options options = new Options();
        options.addOption(CLIOptions.from, true, "Aggregation start date (e.g.: 2018-09-01)");
        options.addOption(CLIOptions.until, true, "Aggregation end date (e.g.: 2018-09-03)");
        options.addOption(CLIOptions.duration, true, "Timeslot duration in minutes (e.g.: 15)");
        options.addOption(CLIOptions.meta, true, "Path to csv file with metadata (e.g.: ./meta.csv)");
        options.addOption(CLIOptions.values, true, "Path to csv file with values (e.g.: ./values.csv)");
        options.addOption(CLIOptions.out, true, "Path for aggregation output (e.g.: ./out");
        return options;
    }

    private static void exitIfMissingOptions(CommandLine cmd, Options options) {
        final Set<String> missingOptions = options.getOptions().stream().map(Option::getOpt)
                .filter(option -> !cmd.hasOption(option))
                .collect(Collectors.toUnmodifiableSet());

        if (missingOptions.size() > 0) {
            System.err.println("Missing required arguments: " + missingOptions);
            new HelpFormatter().printHelp("java -jar sensorReadsAggregation.jar",
                    "Required args:", options, "-from 2018-03-23 -until 2018-03-23 -duration 15 " +
                            "-meta datasets/meta.csv -values datasets/values.csv -out out");
            System.exit(-1);
        }
    }
}
