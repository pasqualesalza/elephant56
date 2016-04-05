package it.unisa.elephant56.core.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import it.unisa.elephant56.core.reporter.individual.IndividualReporter;
import it.unisa.elephant56.core.reporter.time.GeneticOperatorsTimeReporter;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.core.reporter.time.MapReduceTimeReporter;
import it.unisa.elephant56.user.operators.*;
import it.unisa.elephant56.user.operators.Initialisation;
import it.unisa.elephant56.user.operators.TerminationConditionCheck;
import it.unisa.elephant56.util.common.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.avro.mapred.AvroKey;

import it.unisa.elephant56.core.Constants;
import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.FitnessEvaluation;

/**
 * Computes all the Genetic Algorithm functions.
 */
public class IslandMapper
        extends Mapper<AvroKey<IndividualWrapper<Individual, FitnessValue>>, IntWritable,
        AvroKey<IndividualWrapper<Individual, FitnessValue>>, IntWritable> {

    // GenerationsExecutor object.
    private IslandDistributedGenerationsBlockExecutor generationsBlockExecutor;

	// Configuration object.
	private Configuration configuration;

	// Configuration variables.
    private boolean isInitialisationActive;

    private boolean isMigrationActive;

	private boolean isTimeReporterActive;
	private GeneticOperatorsTimeReporter geneticOperatorsTimeReporter;
    private GenerationsBlockTimeReporter generationsBlockTimeReporter;
    private MapReduceTimeReporter mapreduceTimeReporter;
    private Path mapreduceMapperPartialTimeReportFilePath;

	private boolean isIndividualReporterActive;
	private IndividualReporter individualReporter;
	
	// Task objects.
	private FileSystem fileSystem;

    private long generationsBlockNumber;

	private int islandNumber;
	private int totalNumberOfIslands;
	
	private List<IndividualWrapper<Individual, FitnessValue>> inputPopulation;

	private Properties userProperties;

    @SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException {
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
            Path geneticOperatorsTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.GENETIC_OPERATORS_TIME_REPORT_FILE_NAME_FORMAT, this.islandNumber));
            this.geneticOperatorsTimeReporter =
                    new GeneticOperatorsTimeReporter(geneticOperatorsTimeReportFilePath, this.fileSystem);
            this.geneticOperatorsTimeReporter.initialiseFile();

            Path generationsBlockTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.GENERATIONS_BLOCK_TIME_REPORT_FILE_NAME_FORMAT, this.islandNumber));
            this.generationsBlockTimeReporter =
                    new GenerationsBlockTimeReporter(generationsBlockTimeReportFilePath, this.fileSystem);
            this.generationsBlockTimeReporter.initialiseFile();

            this.mapreduceMapperPartialTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.MAPREDUCE_MAPPER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT, this.islandNumber));
            this.mapreduceTimeReporter = new MapReduceTimeReporter(null, this.fileSystem);
        }

        // Checks if the fitness value reporter is active.
        this.isIndividualReporterActive =
                this.configuration.getBoolean(Constants.CONFIGURATION_INDIVIDUAL_REPORTER_ACTIVE, false);
        if (this.isIndividualReporterActive) {
            Path individualReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.INDIVIDUAL_REPORT_FILE_NAME_FORMAT, this.islandNumber));
            this.individualReporter = new IndividualReporter(individualReportFilePath, this.fileSystem);
            this.individualReporter.initialiseFile();
        }

        // Writes some partials.
        if (this.isTimeReporterActive) {
            // Writes the mapper initialisation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_INITIALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the mapper computation start time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
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
        this.isInitialisationActive =
                this.configuration.getBoolean(Constants.CONFIGURATION_INITIALISATION_ACTIVE, false);
        boolean isElitismActive = this.configuration.getBoolean(Constants.CONFIGURATION_ELITISM_ACTIVE, false);
        boolean isSurvivalSelectionActive = this.configuration.getBoolean(Constants.CONFIGURATION_SURVIVAL_SELECTION_ACTIVE, false);
        this.isMigrationActive = this.configuration.getBoolean(Constants.CONFIGURATION_MIGRATION_ACTIVE, false);

        // Retrieves the initialisation population size.
        int initialisationPopulationSize =
                this.configuration.getInt(Constants.CONFIGURATION_INITIALISATION_POPULATION_SIZE, 0);

        // Retrieves the generation numbers.
        long startGenerationNumber = this.configuration.getLong(Constants.CONFIGURATION_START_GENERATION_NUMBER, 0L);
        long finishGenerationNumber = this.configuration.getLong(Constants.CONFIGURATION_FINISH_GENERATION_NUMBER, 0L);

        // Retrieves termination condition satisfaction flag objects.
        Path terminationFlagFilesFolderPath =
                new Path(this.configuration.get(Constants.CONFIGURATION_TERMINATION_FLAG_FILES_FOLDER_PATH));

        // Retrieves the islands properties files folder path.
        Path islandsPropertiesFolderPath =
                new Path(this.configuration.get(Constants.CONFIGURATION_ISLAND_PROPERTIES_FILES_FOLDER_PATH));

        // Retrieves the generations names format.
        String generationsNamesFormat = this.configuration.get(Constants.CONFIGURATION_GENERATION_NAME_FORMAT);

        // Reads the island user properties.
        this.userProperties = new Properties(this.configuration);

        // Creates the GenerationsBlockExecutor object.
        this.generationsBlockExecutor = new IslandDistributedGenerationsBlockExecutor(terminationFlagFilesFolderPath,
                islandsPropertiesFolderPath, this.fileSystem, generationsNamesFormat);

        this.generationsBlockExecutor.setConfiguration(this.configuration);
        this.generationsBlockExecutor.setUserProperties(this.userProperties);

        this.generationsBlockExecutor.setNodeNumber(this.islandNumber);
        this.generationsBlockExecutor.setTotalNumberOfNodes(this.totalNumberOfIslands);

        this.generationsBlockExecutor.setGenerationsBlockNumber(this.generationsBlockNumber);

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

        this.generationsBlockExecutor.activateInitialisation(isInitialisationActive);
        this.generationsBlockExecutor.activateElitism(isElitismActive);
        this.generationsBlockExecutor.activateSurvivalSelection(isSurvivalSelectionActive);
        this.generationsBlockExecutor.activateMigration(isMigrationActive);

        this.generationsBlockExecutor.activateTimeReporter(this.isTimeReporterActive);
        this.generationsBlockExecutor.setGeneticOperatorsTimeReporter(this.geneticOperatorsTimeReporter);
        this.generationsBlockExecutor.setGenerationsBlockTimeReporter(this.generationsBlockTimeReporter);
        this.generationsBlockExecutor.setIndividualReporter(this.individualReporter);

        this.generationsBlockExecutor.activateIndividualReporter(this.isIndividualReporterActive);

        this.generationsBlockExecutor.setInitialisationPopulationSize(initialisationPopulationSize);

        // Instantiates the population.
        this.inputPopulation = new ArrayList<IndividualWrapper<Individual, FitnessValue>>();
    }
	
	@Override
	protected void map(AvroKey<IndividualWrapper<Individual, FitnessValue>> key, IntWritable value, Context context) {
        // Adds the current individual to the population.
        IndividualWrapper<Individual, FitnessValue> individualWrapperClone = null;
        try {
            individualWrapperClone = (IndividualWrapper<Individual, FitnessValue>) key.datum().clone();
        } catch (CloneNotSupportedException exception) {
            exception.printStackTrace();
            System.exit(-1);
        }

        this.inputPopulation.add(individualWrapperClone);
    }

    /**
     * Executes the Genetic Algorithm functions.
     */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
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

        // Retrieves the migration assignments.
        List<Pair<IndividualWrapper<Individual, FitnessValue>, Integer>> migrationAssignments =
                this.generationsBlockExecutor.getMigrationAssignments();

        // Sends the individual to reducer.
        AvroKey<IndividualWrapper<Individual, FitnessValue>> outputKey =
                new AvroKey<IndividualWrapper<Individual, FitnessValue>>();
        IntWritable outputValue = new IntWritable(this.islandNumber);

        // Firstly, sends the migrants.
        if (this.isMigrationActive) {
            for (Pair<IndividualWrapper<Individual, FitnessValue>, Integer> assignment : migrationAssignments) {
                IndividualWrapper<Individual, FitnessValue> currentIndividual = assignment.getFirstElement();
                int currentDestination = assignment.getSecondElement();

                // Removes the individual from the list.
                outputPopulation.remove(currentIndividual);

                outputKey.datum(currentIndividual);
                outputValue.set(currentDestination);
                context.write(outputKey, outputValue);
            }
        }

        // Sends the other individuals, with default assignment.
        for (IndividualWrapper<Individual, FitnessValue> currentIndividual : outputPopulation) {
            outputKey.datum(currentIndividual);
            outputValue.set(this.islandNumber);
            context.write(outputKey, outputValue);
        }

        // Writes some partials.
        if (this.isTimeReporterActive) {
            // Writes the mapper computation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the mapper finalisation start time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_FINALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            // Writes the reducer finalisation start time.
            this.mapreduceTimeReporter.addPartialTime(this.islandNumber, this.generationsBlockNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_INITIALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            // Writes the partial file.
            this.mapreduceTimeReporter.writePartialTimesToFile(this.mapreduceMapperPartialTimeReportFilePath,
                    this.fileSystem);
            // Finalises the files.
            this.geneticOperatorsTimeReporter.finaliseFile();
            this.generationsBlockTimeReporter.finaliseFile();
        }

        if (this.isIndividualReporterActive) {
            // Finalises the file.
            this.individualReporter.finaliseFile();
        }
	}
}
