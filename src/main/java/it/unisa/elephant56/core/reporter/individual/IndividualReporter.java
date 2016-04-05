package it.unisa.elephant56.core.reporter.individual;

import it.unisa.elephant56.core.reporter.time.TimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Locale;

public class IndividualReporter extends TimeReporter {
    /**
     * The header.
     */
    public static final String[] HEADER = {
            "island_number", "generations_block_number", "generation_number",
            "individual", "fitness_value", "termination_condition_satisfied"
    };

    public IndividualReporter(Path outputFilePath, FileSystem fileSystem) throws IOException {
        super(outputFilePath, fileSystem, HEADER);
    }

    public void writeIndividual(
            int islandNumber, long generationsBlockNumber, long generationNumber, Individual individual,
            FitnessValue fitnessValue, boolean terminationConditionSatisfied) {
        String[] line = {
                Integer.toString(islandNumber),
                Long.toString(generationsBlockNumber),
                Long.toString(generationNumber),
                individual.toString(),
                fitnessValue.toString(),
                Boolean.toString(terminationConditionSatisfied)
        };

        super.writeLine(line);
    }
}
