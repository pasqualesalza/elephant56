package it.unisa.elephant56.core.reporter.time;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class GenerationsBlockTimeReporter extends TimeReporter {
    /**
     * The header.
     */
    public static final String[] HEADER = {
            "island_number", "generations_block_number", "generation_number", "phase_type",
            "start_time", "finish_time", "total_time", "human_total_time"
    };

    /**
     * The types of phases.
     */
    public static enum PhaseType {
        //TODO nuova
        INITIALISATION,
        GENERATION,
        GENERATIONS_BLOCK
    };

    public GenerationsBlockTimeReporter(Path outputFilePath, FileSystem fileSystem) throws IOException {
        super(outputFilePath, fileSystem, HEADER);
    }

    public void writeTime(
            int islandNumber, long generationsBlockNumber, long generationNumber, PhaseType phaseType, long startTime,
            long finishTime) {
        super.writeTime(islandNumber, generationsBlockNumber, generationNumber, phaseType.name(), startTime,
                finishTime);
    }
}
