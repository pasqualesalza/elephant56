package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.Constants;
import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.core.reporter.time.GeneticOperatorsTimeReporter;
import it.unisa.elephant56.core.reporter.time.MapReduceTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes all the Genetic Algorithm functions.
 */
public class GridMapper
        extends Mapper<AvroKey<IndividualWrapper<Individual, FitnessValue>>, IntWritable,
        AvroKey<IndividualWrapper<Individual, FitnessValue>>, IntWritable> {

    // GenerationsExecutor object.
    private GridDistributedPrimaryGenerationsBlockExecutor generationsBlockExecutor;

    // Configuration object.
    private Configuration configuration;

    // Configuration variables.
    private boolean isInitialisationActive;

    private boolean isTimeReporterActive;
    private GeneticOperatorsTimeReporter geneticOperatorsTimeReporter;
    private GenerationsBlockTimeReporter generationsBlockTimeReporter;
    private MapReduceTimeReporter mapreduceTimeReporter;
    private Path mapreduceMapperPartialTimeReportFilePath;

    // Task objects.
    private FileSystem fileSystem;

    private long generationNumber;

    private int nodeNumber;
    private int totalNumberOfNodes;

    private List<IndividualWrapper<Individual, FitnessValue>> inputPopulation;

    private Properties userProperties;

    private List<Integer> partitionerNodesDestinations;

    @SuppressWarnings("unchecked")
    @Override
    protected void setup(Context context) throws IOException {
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

            Path generationsBlockTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.GENERATIONS_BLOCK_TIME_REPORT_FILE_NAME_FORMAT, this.nodeNumber));
            this.generationsBlockTimeReporter =
                    new GenerationsBlockTimeReporter(generationsBlockTimeReportFilePath, this.fileSystem);
            this.generationsBlockTimeReporter.initialiseFile();

            this.mapreduceMapperPartialTimeReportFilePath = new Path(reportsFolderPath,
                    String.format(Constants.MAPREDUCE_MAPPER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT, this.nodeNumber));
            this.mapreduceTimeReporter = new MapReduceTimeReporter(null, this.fileSystem);
        }

        // Writes some partials.
        if (this.isTimeReporterActive) {
            // Writes the mapper initialisation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_INITIALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the mapper computation start time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
        }

        // Retrieves the Genetic Algorithm classes defined by the user.
        Class<? extends Initialisation> initialisationClass = (Class<? extends Initialisation>)
                this.configuration.getClass(Constants.CONFIGURATION_INITIALISATION_CLASS, null);
        Class<? extends FitnessEvaluation> fitnessEvaluationClass = (Class<? extends FitnessEvaluation>)
                this.configuration.getClass(Constants.CONFIGURATION_FITNESS_EVALUATION_CLASS, null);

        // Retrieves if doing some operations.
        this.isInitialisationActive =
                this.configuration.getBoolean(Constants.CONFIGURATION_INITIALISATION_ACTIVE, false);

        // Retrieves the initialisation population size.
        int initialisationPopulationSize =
                this.configuration.getInt(Constants.CONFIGURATION_INITIALISATION_POPULATION_SIZE, 0);

        // Retrieves the generation numbers.
        long startGenerationNumber = this.configuration.getLong(Constants.CONFIGURATION_START_GENERATION_NUMBER, 0L);
        long finishGenerationNumber = this.configuration.getLong(Constants.CONFIGURATION_FINISH_GENERATION_NUMBER, 0L);

        // Retrieves the generations names format.
        String generationsNamesFormat = this.configuration.get(Constants.CONFIGURATION_GENERATION_NAME_FORMAT);

        // Reads the island user properties.
        this.userProperties = new Properties(this.configuration);

        // Creates the GenerationsBlockExecutor object.
        this.generationsBlockExecutor = new GridDistributedPrimaryGenerationsBlockExecutor(
                this.fileSystem, generationsNamesFormat);

        this.generationsBlockExecutor.setConfiguration(this.configuration);
        this.generationsBlockExecutor.setUserProperties(this.userProperties);

        this.generationsBlockExecutor.setNodeNumber(this.nodeNumber);
        this.generationsBlockExecutor.setTotalNumberOfNodes(this.totalNumberOfNodes);

        this.generationsBlockExecutor.setGenerationsBlockNumber(this.generationNumber);

        this.generationsBlockExecutor.setStartGenerationNumber(startGenerationNumber);
        this.generationsBlockExecutor.setFinishGenerationNumber(finishGenerationNumber);

        this.generationsBlockExecutor.setInitiliasationClass(initialisationClass);
        this.generationsBlockExecutor.setFitnessEvaluationClass(fitnessEvaluationClass);

        this.generationsBlockExecutor.activateInitialisation(isInitialisationActive);

        this.generationsBlockExecutor.activateTimeReporter(this.isTimeReporterActive);
        this.generationsBlockExecutor.setGeneticOperatorsTimeReporter(this.geneticOperatorsTimeReporter);
        this.generationsBlockExecutor.setGenerationsBlockTimeReporter(this.generationsBlockTimeReporter);

        this.generationsBlockExecutor.setInitialisationPopulationSize(initialisationPopulationSize);

        // Instantiates the population.
        this.inputPopulation = new ArrayList<>();

        // Instantiates the partitioner nodes destionation list.
        this.partitionerNodesDestinations = new ArrayList<>();
    }

    @Override
    protected void map(AvroKey<IndividualWrapper<Individual, FitnessValue>> key, IntWritable value, Context context) {
        if (!this.isInitialisationActive) {
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

        // Adds the destination to the partitioner nodes destinations list.
        this.partitionerNodesDestinations.add(value.get());
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

        // Sends the individual to reducer.
        AvroKey<IndividualWrapper<Individual, FitnessValue>> outputKey =
                new AvroKey<IndividualWrapper<Individual, FitnessValue>>();
        IntWritable outputValue = new IntWritable();

        for (int i = 0; i < outputPopulation.size(); i++) {
            IndividualWrapper<Individual, FitnessValue> currentIndividual = outputPopulation.get(i);
            int currentPartitionerNodeDestination = this.partitionerNodesDestinations.get(i);

            outputKey.datum(currentIndividual);
            outputValue.set(currentPartitionerNodeDestination);

            context.write(outputKey, outputValue);
        }

        // Writes some partials.
        if (this.isTimeReporterActive) {
            // Writes the mapper computation finish time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_COMPUTATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.FINISH, System.currentTimeMillis());
            // Writes the mapper finalisation start time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.MAPPER_FINALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            // Writes the reducer finalisation start time.
            this.mapreduceTimeReporter.addPartialTime(this.nodeNumber, this.generationNumber,
                    MapReduceTimeReporter.PhaseType.REDUCER_INITIALISATION,
                    MapReduceTimeReporter.PartialTimeKey.Type.START, System.currentTimeMillis());
            // Writes the partial file.
            this.mapreduceTimeReporter.writePartialTimesToFile(this.mapreduceMapperPartialTimeReportFilePath,
                    this.fileSystem);
            // Finalises the files.
            this.geneticOperatorsTimeReporter.finaliseFile();
            this.generationsBlockTimeReporter.finaliseFile();
        }
    }
}
