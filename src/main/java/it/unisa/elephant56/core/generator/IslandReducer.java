package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.Constants;
import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.reporter.time.MapReduceTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IslandReducer
        extends Reducer<AvroKey<IndividualWrapper<Individual, FitnessValue>>, IntWritable,
        AvroKey<IndividualWrapper<Individual, FitnessValue>>, NullWritable> {

    // GenerationsExecutor object.
    private GenerationsBlockExecutor generationsBlockExecutor;

	// Configuration object.
	private Configuration configuration;

	// Configuration variables.
    private boolean isMigrationActive;

    private boolean isTimeReporterActive;
    private MapReduceTimeReporter mapreduceTimeReporter;
    private Path mapreduceReducerPartialTimeReportFilePath;

    // Avro Multiple Outputs object.
    private AvroMultipleOutputs avroMultipleOutputs;

	// Task objects.
	FileSystem fileSystem;

    private long generationsBlockNumber;

    private int islandNumber;
    private int totalNumberOfIslands;

    private List<IndividualWrapper<Individual, FitnessValue>> inputPopulation;

    private boolean isTerminationConditionSatisfied;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// Reads the configuration from the context.
		this.configuration = context.getConfiguration();

        // Reads the task number.
        this.islandNumber = configuration.getInt("mapred.task.partition", 0);

        // Reads the total number of tasks.
        this.totalNumberOfIslands = configuration.getInt("mapred.map.tasks", 1);

        // Retrieves the filesystem.
        this.fileSystem = FileSystem.get(this.configuration);

        // Retrieves the generations block number.
        this.generationsBlockNumber = this.configuration.getLong(Constants.CONFIGURATION_GENERATIONS_BLOCK_NUMBER, 0L);

        // Reads the reports folder path.
        Path reportsFolderPath = new Path(this.configuration.get(Constants.CONFIGURATION_REPORTS_FOLDER_PATH));

        // Checks if the time reporter is active.
        this.isTimeReporterActive = this.configuration.getBoolean(Constants.CONFIGURATION_TIME_REPORTER_ACTIVE, false);
        if (this.isTimeReporterActive) {
            // Creates the reporter.
            this.mapreduceReducerPartialTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.MAPREDUCE_REDUCER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT, this.islandNumber));
            this.mapreduceTimeReporter = new MapReduceTimeReporter(null, this.fileSystem);

            // Writes the mapper finalisation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_FINALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the reducer initialisation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_INITIALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the reducer computation start time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            this.mapreduceTimeReporter.writePartialTimesToFile(this.mapreduceReducerPartialTimeReportFilePath,
                    this.fileSystem);
        }

        // Retrieves if doing some operations.
        this.isMigrationActive = this.configuration.getBoolean(Constants.CONFIGURATION_MIGRATION_ACTIVE, false);

        // Retrieves termination condition satisfaction flag objects.
        Path terminationFlagFilesFolderPath = new Path(
                this.configuration.get(Constants.CONFIGURATION_TERMINATION_FLAG_FILES_FOLDER_PATH));

        // Retrieves the islands properties files folder path.
        Path islandsPropertiesFolderPath = new Path(
                this.configuration.get(Constants.CONFIGURATION_ISLAND_PROPERTIES_FILES_FOLDER_PATH));

        // Retrieves the generations names format.
        String generationsNamesFormat = this.configuration.get(Constants.CONFIGURATION_GENERATION_NAME_FORMAT);

        // Creates the GenerationsBlockExecutor object.
        this.generationsBlockExecutor = new IslandDistributedGenerationsBlockExecutor(terminationFlagFilesFolderPath,
                islandsPropertiesFolderPath, this.fileSystem, generationsNamesFormat);

        // Instantiates the population.
        this.inputPopulation = new ArrayList<IndividualWrapper<Individual, FitnessValue>>();

        // Checks if a termination condition satisfaction occurred.
        this.isTerminationConditionSatisfied =
                generationsBlockExecutor.checkTerminationConditionSatisfactionNotifications();
	}

    @Override
    protected void reduce(AvroKey<IndividualWrapper<Individual, FitnessValue>> key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        // Adds the current individual to the population.
        IndividualWrapper<Individual, FitnessValue> individualWrapperClone = null;
        try {
            individualWrapperClone = (IndividualWrapper<Individual, FitnessValue>) key.datum().clone();
        } catch (CloneNotSupportedException exception) {
            exception.printStackTrace();
            System.exit(-1);
        } catch (ClassCastException exception) {
            exception.printStackTrace();
            System.exit(-1);
        }

        this.inputPopulation.add(individualWrapperClone);
    }

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
        // Sorts the population.
        Collections.reverse(this.inputPopulation);

        // Sends the individual to HDFS.
        AvroKey<IndividualWrapper<Individual, FitnessValue>> outputKey =
                new AvroKey<IndividualWrapper<Individual, FitnessValue>>();
        for (IndividualWrapper<Individual, FitnessValue> currentIndividual : this.inputPopulation) {
            outputKey.datum(currentIndividual);
            context.write(outputKey, NullWritable.get());
        }

        /* TODO Implementare solutions filter
        // Check if filter the population (termination or last generation).
        if (this.isTerminationConditionSatisfied || !this.isMigrationActive) {
            // Filters the solutions.
            this.generationsBlockExecutor.setInputPopulation(this.inputPopulation);
            this.generationsBlockExecutor.filterSolutions();

            // Creates the Avro Multiple Outputs object.
            AvroMultipleOutputs avroMultipleOutputs = new AvroMultipleOutputs(context);

            // Retrieves the output.
            List<IndividualWrapper<Individual, FitnessValue>> solutionsPopulation = this.generationsBlockExecutor.getSolutionsPopulation();
            List<IndividualWrapper<Individual, FitnessValue>> nonSolutionsPopulation = this.generationsBlockExecutor.getNonsolutionsPopulation();

            for (IndividualWrapper<Individual, FitnessValue> currentIndividual : solutionsPopulation) {
                outputKey.datum(currentIndividual);
                avroMultipleOutputs.write(Constants.CONFIGURATION_SOLUTIONS_NAME, outputKey);
            }

            for (IndividualWrapper<Individual, FitnessValue> currentIndividual : nonSolutionsPopulation) {
                outputKey.datum(currentIndividual);
                avroMultipleOutputs.write(Constants.CONFIGURATION_NONSOLUTIONS_NAME, outputKey);
            }
        }
        */

        // Writes some partials.
        if (this.isTimeReporterActive) {
            // Writes the reducer computation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the reducer finalisation start time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_FINALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            // Writes the partial file.
            this.mapreduceTimeReporter.writePartialTimesToFile(this.mapreduceReducerPartialTimeReportFilePath,
                    this.fileSystem);
        }
	}
}
