package it.unisa.elephant56.core.reporter.time;

import it.unisa.elephant56.core.reporter.Reporter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TimeReporter extends Reporter {
    public TimeReporter(Path outputFilePath, FileSystem fileSystem, String[] header) throws IOException {
        super(outputFilePath, fileSystem, header);
    }

    public void writeTime(
            int islandNumber, long generationsBlockNumber, long generationNumber, String phaseType, long startTime,
            long finishTime
    ) {
        long totalTime = finishTime - startTime;

        String[] line = {
                Integer.toString(islandNumber),
                Long.toString(generationsBlockNumber),
                Long.toString(generationNumber),
                phaseType,
                Long.toString(startTime),
                Long.toString(finishTime),
                Long.toString(totalTime),
                convertToHumansTime(totalTime)
        };

        super.writeLine(line);
    }

    /**
     * Converts the time in millisenconds in a human readable form "hh:mm:ss".
     *
     * @param msTime the time in milliseconds
     *
     * @return the human readable string
     */
    public static String convertToHumansTime(long msTime) {
        long hours = TimeUnit.MILLISECONDS.toHours(msTime);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(msTime);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(msTime);

        long hoursToWrite = hours;
        long minutesToWrite = minutes
                - TimeUnit.HOURS.toMinutes(hours);
        long secondsToWrite = seconds
                - TimeUnit.MINUTES.toSeconds(minutes);
        long millisecondsToWrite = msTime
                - TimeUnit.SECONDS.toMillis(seconds);

        String humanTimeString = String.format("%02d:%02d:%02d.%03d",
                hoursToWrite,
                minutesToWrite,
                secondsToWrite,
                millisecondsToWrite
        );

        return humanTimeString;
    }
}
