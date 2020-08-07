package com.pogorelovs.sensor;

import com.pogorelovs.sensor.constant.CLIOptions;
import org.apache.commons.cli.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws ParseException {
        Options options = populateCliOptions();

        CommandLine providedArgs = new DefaultParser().parse(options, args);
        exitIfMissingOptions(providedArgs, options);

        final LocalDate from = LocalDate.parse(providedArgs.getOptionValue(CLIOptions.from));
        final LocalDate until = LocalDate.parse(providedArgs.getOptionValue(CLIOptions.until));
        final Duration duration = Duration.ofMinutes(Integer.parseInt(providedArgs.getOptionValue(CLIOptions.duration)));
        final Path metadataPath = Paths.get(providedArgs.getOptionValue(CLIOptions.meta));
        final Path valuesPath = Paths.get(providedArgs.getOptionValue(CLIOptions.values));
        final Path outputPath = Paths.get(providedArgs.getOptionValue(CLIOptions.out));

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
                    "Required args:", options,
                    "Example: -from 2018-09-01 -until 2018-09-03 -duration 15 " +
                            "-meta datasets/meta.csv -values datasets/values.csv -out out");
            System.exit(-1);
        }
    }
}
