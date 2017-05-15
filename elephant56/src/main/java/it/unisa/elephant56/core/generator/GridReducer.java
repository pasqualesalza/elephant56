package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.Constants;
import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.core.reporter.individual.IndividualReporter;
import it.unisa.elephant56.core.reporter.time.GeneticOperatorsTimeReporter;
import it.unisa.elephant56.core.reporter.time.MapReduceTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;
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

public class GridReducer
        extends Reducer<AvroKey<IndividualWrapper<Individual, FitnessValue>>, IntWritable,
        AvroKey<IndividualWrapper<Individual, FitnessValue>>, NullWritable> {

    // GenerationsExecutor object.
    private GridDistributedSecondaryGenerationsBlockExecutor generationsBlockExecutor;

    // Configuration object.
    private Configuration configuration;

    // Configuration variables.
    private boolean isTimeReporterActive;
    private GeneticOperatorsTimeReporter geneticOperatorsTimeReporter;
    private MapReduceTimeReporter mapreduceTimeReporter;
    private Path mapreduceReducerPartialTimeReportFilePath;

    private boolean isIndividualReporterActive;
    private IndividualReporter individualReporter;

    // Avro Multiple Outputs object.
    private AvroMultipleOutputs avroMultipleOutputs;

    // Task objects.
    FileSystem fileSystem;

    private long generationNumber;

    private int nodeNumber;
    private int totalNumberOfNodes;

    private List<IndividualWrapper<Individual, FitnessValue>> inputPopulation;

    private Properties userProperties;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Reads the configuration from the context.
        this.configuration = context.getConfiguration();

        // Reads the task number.
        this.nodeNumber = configuration.getInt("mapred.task.partition", 0);

        // Reads the total number of tasks.
        this.totalNumberOfNodes = configuration.getInt("mapred.map.tasks", 1);

        // Retrieves the filesystem.
        this.fileSystem = FileSystem.get(this.configuration);

        // Retrieves the generations block number.
        this.generationNumber = this.configuration.getLong(Constants.CONFIGURATION_GENERATIONS_BLOCK_NUMBER, 0L);

        // Reads the reports folder path.
        Path reportsFolderPath = new Path(this.configuration.get(Constants.CONFIGURATION_REPORTS_FOLDER_PATH));

        // Checks if the time reporter is active.
        this.isTimeReporterActive = this.configuration.getBoolean(Constants.CONFIGURATION_TIME_REPORTER_ACTIVE, false);
        if (this.isTimeReporterActive) {
            Path geneticOperatorsTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.GENETIC_OPERATORS_TIME_REPORT_FILE_NAME_FORMAT, this.nodeNumber));
            this.geneticOperatorsTimeReporter =
                    new GeneticOperatorsTimeReporter(geneticOperatorsTimeReportFilePath, this.fileSystem);
            this.geneticOperatorsTimeReporter.initialiseFile();

            this.mapreduceReducerPartialTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.MAPREDUCE_REDUCER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT, this.nodeNumber));
            this.mapreduceTimeReporter = new MapReduceTimeReporter(null, this.fileSystem);

            // Writes the mapper finalisation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_FINALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the reducer initialisation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_INITIALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the reducer computation start time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            this.mapreduceTimeReporter.writePartialTimesToFile(this.mapreduceReducerPartialTimeReportFilePath,
                    this.fileSystem);
        }

        // Checks if the fitness value reporter is active.
        this.isIndividualReporterActive =
                this.configuration.getBoolean(Constants.CONFIGURATION_INDIVIDUAL_REPORTER_ACTIVE, false);
        if (this.isIndividualReporterActive) {
            Path individualReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.INDIVIDUAL_REPORT_FILE_NAME_FORMAT, this.nodeNumber));
            this.individualReporter = new IndividualReporter(individualReportFilePath, this.fileSystem);
            this.individualReporter.initialiseFile();
        }

        // Retrieves the Genetic Algorithm classes defined by the user.
        Class<? extends Initialisation> initialisationClass = (Class<? extends Initialisation>)
                this.configuration.getClass(Constants.CONFIGURATION_INITIALISATION_CLASS, null);
        Class<? extends FitnessEvaluation> fitnessEvaluationClass = (Class<? extends FitnessEvaluation>)
                this.configuration.getClass(Constants.CONFIGURATION_FITNESS_EVALUATION_CLASS, null);
        Class<? extends TerminationConditionCheck> terminationConditionCheckClass =
                (Class<? extends TerminationConditionCheck>)
                        this.configuration.getClass(Constants.CONFIGURATION_TERMINATION_CONDITION_CHECK_CLASS, null);
        Class<? extends Elitism> elitismClass = (Class<? extends Elitism>)
                this.configuration.getClass(Constants.CONFIGURATION_ELITISM_CLASS, null);
        Class<? extends ParentsSelection> parentsSelectionClass = (Class<? extends ParentsSelection>)
                this.configuration.getClass(Constants.CONFIGURATION_PARENTS_SELECTION_CLASS, null);
        Class<? extends Crossover> crossoverClass = (Class<? extends Crossover>)
                this.configuration.getClass(Constants.CONFIGURATION_CROSSOVER_CLASS, null);
        Class<? extends Mutation> mutationClass = (Class<? extends Mutation>)
                this.configuration.getClass(Constants.CONFIGURATION_MUTATION_CLASS, null);
        Class<? extends SurvivalSelection> survivalSelectionClass = (Class<? extends SurvivalSelection>)
                this.configuration.getClass(Constants.CONFIGURATION_SURVIVAL_SELECTION_CLASS, null);
        Class<? extends Migration> migrationClass = (Class<? extends Migration>)
                this.configuration.getClass(Constants.CONFIGURATION_MIGRATION_CLASS, null);

        // Retrieves if doing some operations.
        boolean isElitismActive = this.configuration.getBoolean(Constants.CONFIGURATION_ELITISM_ACTIVE, false);
        boolean isSurvivalSelectionActive = this.configuration.getBoolean(Constants.CONFIGURATION_SURVIVAL_SELECTION_ACTIVE, false);

        // Retrieves the generation numbers.
        long startGenerationNumber = this.configuration.getLong(Constants.CONFIGURATION_START_GENERATION_NUMBER, 0L);
        long finishGenerationNumber = this.configuration.getLong(Constants.CONFIGURATION_FINISH_GENERATION_NUMBER, 0L);

        // Reads if it is the last generation.
        boolean isLastGeneration = this.configuration.getBoolean(Constants.CONFIGURATION_GRID_PARALLELISATION_MODEL_IS_LAST_GENERATION, false);

        // Reads the island user properties.
        this.userProperties = new Properties(this.configuration);

        // Retrieves the generations names format.
        String generationsNamesFormat = this.configuration.get(Constants.CONFIGURATION_GENERATION_NAME_FORMAT);

        // Creates the GenerationsBlockExecutor object.
        this.generationsBlockExecutor = new GridDistributedSecondaryGenerationsBlockExecutor(isLastGeneration);

        this.generationsBlockExecutor.setConfiguration(this.configuration);
        this.generationsBlockExecutor.setUserProperties(this.userProperties);

        this.generationsBlockExecutor.setNodeNumber(this.nodeNumber);
        this.generationsBlockExecutor.setTotalNumberOfNodes(this.totalNumberOfNodes);

        this.generationsBlockExecutor.setGenerationsBlockNumber(this.generationNumber);

        this.generationsBlockExecutor.setStartGenerationNumber(startGenerationNumber);
        this.generationsBlockExecutor.setFinishGenerationNumber(finishGenerationNumber);

        this.generationsBlockExecutor.setInitiliasationClass(initialisationClass);
        this.generationsBlockExecutor.setFitnessEvaluationClass(fitnessEvaluationClass);
        this.generationsBlockExecutor.setTerminationConditionCheckClass(terminationConditionCheckClass);
        this.generationsBlockExecutor.setElitismClass(elitismClass);
        this.generationsBlockExecutor.setParentsSelectionClass(parentsSelectionClass);
        this.generationsBlockExecutor.setCrossoverClass(crossoverClass);
        this.generationsBlockExecutor.setMutationClass(mutationClass);
        this.generationsBlockExecutor.setSurvivalSelectionClass(survivalSelectionClass);
        this.generationsBlockExecutor.setMigrationClass(migrationClass);

        this.generationsBlockExecutor.activateElitism(isElitismActive);
        this.generationsBlockExecutor.activateSurvivalSelection(isSurvivalSelectionActive);

        this.generationsBlockExecutor.activateTimeReporter(this.isTimeReporterActive);
        this.generationsBlockExecutor.setGeneticOperatorsTimeReporter(this.geneticOperatorsTimeReporter);
        this.generationsBlockExecutor.setIndividualReporter(this.individualReporter);

        this.generationsBlockExecutor.activateIndividualReporter(this.isIndividualReporterActive);

        // Instantiates the population.
        this.inputPopulation = new ArrayList<>();
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

        // Sets the input population.
        this.generationsBlockExecutor.setInputPopulation(this.inputPopulation);

        // Executes the generations.
        try {
            this.generationsBlockExecutor.run();
        } catch (Exception exception) {
            exception.printStackTrace();
        }

        // Retrieves the output.
        List<IndividualWrapper<Individual, FitnessValue>> outputPopulation =
                this.generationsBlockExecutor.getOutputPopulation();

        // Sends the individual to HDFS.
        AvroKey<IndividualWrapper<Individual, FitnessValue>> outputKey =
                new AvroKey<IndividualWrapper<Individual, FitnessValue>>();
        for (IndividualWrapper<Individual, FitnessValue> currentIndividual : outputPopulation) {
            outputKey.datum(currentIndividual);
            context.write(outputKey, NullWritable.get());
        }

        // Writes some partials.
        if (this.isTimeReporterActive) {
            // Writes the reducer computation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the reducer finalisation start time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_FINALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            // Writes the partial file.
            this.mapreduceTimeReporter.writePartialTimesToFile(this.mapreduceReducerPartialTimeReportFilePath,
                    this.fileSystem);
            // Finalises the files.
            this.geneticOperatorsTimeReporter.finaliseFile();
        }

        if (this.isIndividualReporterActive) {
            // Finalises the file.
            this.individualReporter.finaliseFile();
        }
    }
}
