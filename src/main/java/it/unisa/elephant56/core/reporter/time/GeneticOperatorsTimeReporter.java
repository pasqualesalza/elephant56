package it.unisa.elephant56.core.reporter.time;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class GeneticOperatorsTimeReporter extends TimeReporter {
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
        AVERAGE_INITIALISATION,
        TOTAL_INITIALISATION,
        MIN_FITNESS_EVALUATION,
        MAX_FITNESS_EVALUATION,
        AVERAGE_FITNESS_EVALUATION,
        TOTAL_FITNESS_EVALUATION,
        AVERAGE_INDIVIDUAL_TERMINATION_CONDITION_CHECK,
        TOTAL_INDIVIDUAL_TERMINATION_CONDITION_CHECK,
        TOTAL_ISLAND_TERMINATION_CONDITION_CHECK,
        TOTAL_GLOBAL_TERMINATION_CONDITION_CHECK,
        TOTAL_ELITISM,
        TOTAL_PARENTS_SELECTION,
        AVERAGE_CROSSOVER,
        TOTAL_CROSSOVER,
        AVERAGE_MUTATION,
        TOTAL_MUTATION,
        MIN_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION,
        MAX_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION,
        AVERAGE_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION,
        TOTAL_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION,
        TOTAL_SURVIVAL_SELECTION,
        TOTAL_MIGRATION
    };

    public GeneticOperatorsTimeReporter(Path outputFilePath, FileSystem fileSystem) throws IOException {
        super(outputFilePath, fileSystem, HEADER);
    }

    public void writeTime(
            int islandNumber, long generationsBlockNumber, long generationNumber, PhaseType phaseType, long startTime,
            long finishTime) {
        super.writeTime(islandNumber, generationsBlockNumber, generationNumber, phaseType.name(), startTime, finishTime);
    }
}
