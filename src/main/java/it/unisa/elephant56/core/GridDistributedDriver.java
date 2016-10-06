package it.unisa.elephant56.core;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.generator.*;
import it.unisa.elephant56.core.input.NodesInputFormat;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.core.reporter.time.MapReduceTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.operators.*;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.*;

public class GridDistributedDriver extends DistributedDriver {

    // Driver objects.
    private int numberOfNodes;

    private long randomSeed;
    private Random random;

    /**
     * Constructs a new driver.
     *
     * @param applicationName      the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     * @param numberOfNodes        the number of nodes
     * @param randomSeed           the random seed
     */
    public GridDistributedDriver(String applicationName, Class<?> applicationMainClass, int numberOfNodes,
                                 long randomSeed) {
        super(applicationName, applicationMainClass);
        init(numberOfNodes, randomSeed);
    }

    /**
     * Constructs a new driver specifying the configuration,
     * referred only to the filesystem operations.
     *
     * @param applicationName      the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     * @param numberOfNodes        the number of nodes
     * @param randomSeed           the random seed
     * @param configuration        the configuration
     */
    public GridDistributedDriver(
            String applicationName, Class<?> applicationMainClass, int numberOfNodes, long randomSeed,
            Configuration configuration) {
        super(applicationName, applicationMainClass, configuration);
        init(numberOfNodes, randomSeed);
    }

    /**
     * Initialises the driver.
     *
     * @param numberOfNodes the number of nodes
     * @param randomSeed    the random seed
     */
    private void init(int numberOfNodes, long randomSeed) {
        this.numberOfNodes = numberOfNodes;

        this.random = new Random(randomSeed);
    }

    @Override
    public void run() throws Exception {

        // Registers the generations block start time.
        long generationsBlockStartTime = System.currentTimeMillis();

        // Creates the folders.
        this.createFolder(this.getGenerationsBlocksFolderPath(), true);

        int initialisationPopulationSizePerSplit = (int) Math.ceil((double) this.initialisationPopulationSize / (double) this.numberOfNodes);

        // Copies the user properties into configuration.
        Configuration configurationWithUserProperties = new Configuration(this.configuration);
        this.userProperties.copyIntoConfiguration(configurationWithUserProperties);

        // Retrieves the schema.
        Schema individualWrapperSchema = IndividualWrapper.getSchema(this.individualClass, this.fitnessValueClass);

        // Launches the jobs.
        long lastGenerationNumber = maximumNumberOfGenerations - 1L;
        String generationNameFormat = getGenerationNameFormat(lastGenerationNumber);
        this.generationsBlockNameFormat = getGenerationsBlockNameFormat(lastGenerationNumber);
        long currentGenerationNumber = 0L;

        // Creates and initialises the generations block time reporter.
        GenerationsBlockTimeReporter generationsBlockTimeReporter = null;
        if (this.isTimeReporterActive) {
            Path generationsBlockTimeReportFilePath = new Path(this.getReportsFolderPath(),
                    Constants.GRID_GENERATIONS_BLOCK_TIME_REPORT_FILE_NAME_FORMAT);
            generationsBlockTimeReporter =
                    new GenerationsBlockTimeReporter(generationsBlockTimeReportFilePath, this.fileSystem);
            generationsBlockTimeReporter.initialiseFile();
        }

        while (true) {
            // Checks if launching another job.
            if (currentGenerationNumber <= lastGenerationNumber) {
                // Create the folder for the current generations block reports.
                Path currentGenerationReportsFolderPath = new Path(this.getReportsFolderPath(),
                        String.format(generationNameFormat, currentGenerationNumber));
                this.createFolder(currentGenerationReportsFolderPath, true);

                boolean isLastGeneration = false;
                if (currentGenerationNumber == lastGenerationNumber)
                    isLastGeneration = true;

                // Creates the job.
                Job currentGenerationsExecutorJob = this.createJob(
                        configurationWithUserProperties,
                        numberOfNodes,
                        initialisationPopulationSizePerSplit,
                        currentGenerationNumber,
                        isLastGeneration,
                        generationNameFormat,
                        currentGenerationReportsFolderPath,
                        individualWrapperSchema
                );

                // Randomly assigns neighbourhoods destinations.
                List<Integer> positions = new ArrayList<>(this.initialisationPopulationSize);
                List<Integer> partitionerNodesDestinations = new ArrayList<>(this.initialisationPopulationSize);
                for (int i = 0; i < this.initialisationPopulationSize; i++) {
                    positions.add(i, i);
                    partitionerNodesDestinations.add(i, null);
                }
                int currentDestinationIndex = 0;
                while (true) {
                    if (currentDestinationIndex >= this.initialisationPopulationSize)
                        break;

                    for (int currentNode = 0; currentNode < this.numberOfNodes; currentNode++) {
                        if (currentDestinationIndex >= this.initialisationPopulationSize)
                            break;

                        Integer randomPosition = positions.get(this.random.nextInt(positions.size()));
                        positions.remove(randomPosition);

                        partitionerNodesDestinations.set(randomPosition, currentNode);
                        currentDestinationIndex++;
                    }
                }
                NodesInputFormat.setPartitionerNodesDestinations(currentGenerationsExecutorJob, partitionerNodesDestinations);

                // Creates the time reporters.
                MapReduceTimeReporter mapreduceTimeReporter = null;
                if (this.isTimeReporterActive) {
                    // Creates and initialises the MapReduce time reporter.
                    Path mapreduceTimeReportFilePath = new Path(currentGenerationReportsFolderPath,
                            Constants.MAPREDUCE_TIME_REPORT_FILE_NAME_FORMAT);
                    mapreduceTimeReporter = new MapReduceTimeReporter(mapreduceTimeReportFilePath, this.fileSystem);
                    mapreduceTimeReporter.initialiseFile();

                    // Registers the mapper initialisation start partial time.
                    long startTime = System.currentTimeMillis();
                    for (int islandNumber = 0; islandNumber < this.numberOfNodes; islandNumber++)
                        mapreduceTimeReporter.addPartialTime(islandNumber, currentGenerationNumber,
                                MapReduceTimeReporter.PhaseType.MAPPER_INITIALISATION,
                                MapReduceTimeReporter.PartialTimeKey.Type.START, startTime);
                }

                long generationStartTime = System.currentTimeMillis();

                // Launches the job.
                currentGenerationsExecutorJob.waitForCompletion(true);

                // Does some time reporter operations.
                if (this.isTimeReporterActive) {
                    // Registers the reducer finalisation finish partial time.
                    long finishTime = System.currentTimeMillis();
                    for (int nodeNumber = 0; nodeNumber < this.numberOfNodes; nodeNumber++) {
                        mapreduceTimeReporter.addPartialTime(nodeNumber, currentGenerationNumber,
                                MapReduceTimeReporter.PhaseType.REDUCER_FINALISATION,
                                MapReduceTimeReporter.PartialTimeKey.Type.FINISH, finishTime);

                        // Reads the partial times from mapper and reducer files.
                        Path mapreduceMapperPartialTimeReportFilePath = new Path(
                                currentGenerationReportsFolderPath,
                                String.format(Constants.MAPREDUCE_MAPPER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT,
                                        nodeNumber)
                        );
                        mapreduceTimeReporter.addPartialTimesFromFile(mapreduceMapperPartialTimeReportFilePath,
                                this.getFileSystem());

                        Path mapreduceReducerPartialTimeReportFilePath = new Path(
                                currentGenerationReportsFolderPath,
                                String.format(Constants.MAPREDUCE_REDUCER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT,
                                        nodeNumber)
                        );
                        mapreduceTimeReporter.addPartialTimesFromFile(mapreduceReducerPartialTimeReportFilePath,
                                this.getFileSystem());

                        // Joins the partial times.
                        mapreduceTimeReporter.joinPartialTimes(nodeNumber, currentGenerationNumber);
                    }

                    // Writes the reporter files.
                    mapreduceTimeReporter.finaliseFile();

                    // Writes the generation time.
                    generationsBlockTimeReporter.writeTime(0, currentGenerationNumber,
                            currentGenerationNumber, GenerationsBlockTimeReporter.PhaseType.GENERATION,
                            generationStartTime, System.currentTimeMillis());
                }
            }

            // Checks if the maximum number of jobs has been reached.
            if (currentGenerationNumber == lastGenerationNumber)
                break;

            // Checks if it is time to stop for termination condition satisfaction.
            // TODO Riabilitare accesso HDFS.
            //if (DistributedGenerationsBlockExecutor.checkTerminationConditionSatisfaction(
            //        getTerminationFlagsFolderPath(), this.fileSystem))
            //    break;

            // Updates the generation number.
            currentGenerationNumber++;
        }

        // Registers the generations block finish time.
        long generationsBlockFinishTime = System.currentTimeMillis();

        // Writes the generation block time.
        if (this.isTimeReporterActive) {
            generationsBlockTimeReporter.writeTime(0, currentGenerationNumber, -1L,
                    GenerationsBlockTimeReporter.PhaseType.GENERATIONS_BLOCK, generationsBlockStartTime,
                    generationsBlockFinishTime);
        }

        // Writes the reporter files.
        generationsBlockTimeReporter.finaliseFile();

        this.lastExecutedGenerationNumber = currentGenerationNumber;
        this.lastExecutedGenerationsBlockNumber = currentGenerationNumber;
    }

    protected Job createJob(
            Configuration configuration,
            int numberOfNodes,
            int initialisationPopulationSizePerSplit,
            long currentGenerationNumber,
            boolean isLastGeneration,
            String generationNameFormat,
            Path currentGenerationsBlockReportsFolderPath,
            Schema individualWrapperSchema
    ) throws IOException {
        // Creates a job.
        Job job = super.createJob(configuration, numberOfNodes, currentGenerationNumber, currentGenerationNumber,
                (currentGenerationNumber - 1L), currentGenerationNumber, generationNameFormat,
                currentGenerationsBlockReportsFolderPath, individualWrapperSchema,
                GridMapper.class, SpecificNodePartitioner.class, GridReducer.class);

        // Checks if is the first generations block.
        if (currentGenerationNumber == 0L) {
            if (this.isInitialisationActive) {
                NodesInputFormat.setInitialisationPopulationSizePerSplit(job, initialisationPopulationSizePerSplit);
                NodesInputFormat.activateInitialisation(job, true);
                job.getConfiguration().setBoolean(Constants.CONFIGURATION_INITIALISATION_ACTIVE, true);
                job.getConfiguration().setInt(Constants.CONFIGURATION_INITIALISATION_POPULATION_SIZE,
                        initialisationPopulationSizePerSplit);
            } else {
                NodesInputFormat.setInputPopulationFolderPath(job, this.inputPopulationFolderPath);
                NodesInputFormat.activateInitialisation(job, false);
            }
        } else {
            // It is not the first generations block.
            NodesInputFormat.setInputPopulationFolderPath(job, this.getGenerationsBlockFolderPath(
                    (currentGenerationNumber - 1L), this.generationsBlockNameFormat));
            NodesInputFormat.activateInitialisation(job, false);
        }

        // Configures the flag for the last generation.
        job.getConfiguration().setBoolean(Constants.CONFIGURATION_GRID_PARALLELISATION_MODEL_IS_LAST_GENERATION, isLastGeneration);

        // Configures the fitness value class.
        job.getConfiguration().setClass(Constants.CONFIGURATION_FITNESS_VALUE_CLASS, this.fitnessValueClass,
                FitnessValue.class);

        // Configures the Initialisation phase.
        job.getConfiguration().setClass(Constants.CONFIGURATION_INITIALISATION_CLASS, this.initialisationClass,
                Initialisation.class);

        // Configures the Fitness Evaluation phase.
        job.getConfiguration().setClass(Constants.CONFIGURATION_FITNESS_EVALUATION_CLASS, this.fitnessEvaluationClass,
                FitnessEvaluation.class);

        // Configures the Termination phase.
        job.getConfiguration().setClass(Constants.CONFIGURATION_TERMINATION_CONDITION_CHECK_CLASS,
                this.terminationConditionCheckClass, TerminationConditionCheck.class);

        // Configures the Elitism phase.
        if (this.isElitismActive) {
            job.getConfiguration().setClass(Constants.CONFIGURATION_ELITISM_CLASS, this.elitismClass, Elitism.class);
            job.getConfiguration().setBoolean(Constants.CONFIGURATION_ELITISM_ACTIVE, true);
        }

        // Configures the ParentsSelection phase.
        job.getConfiguration().setClass(Constants.CONFIGURATION_PARENTS_SELECTION_CLASS, this.parentsSelectionClass, ParentsSelection.class);

        // Configures the Crossover phase.
        job.getConfiguration().setClass(Constants.CONFIGURATION_CROSSOVER_CLASS, this.crossoverClass, Crossover.class);

        // Configures the Mutation phase.
        job.getConfiguration().setClass(Constants.CONFIGURATION_MUTATION_CLASS, this.mutationClass, Mutation.class);

        // Configures the Survival Selection phase.
        if (this.isSurvivalSelectionActive) {
            job.getConfiguration().setClass(Constants.CONFIGURATION_SURVIVAL_SELECTION_CLASS, this.survivalSelectionClass, SurvivalSelection.class);
            job.getConfiguration().setBoolean(Constants.CONFIGURATION_SURVIVAL_SELECTION_ACTIVE, true);
        }

        // Returns the job.
        return job;
    }

    public FileSystem getFileSystem() throws IOException {
        if (this.fileSystem == null)
            this.fileSystem = FileSystem.get(this.configuration);
        return this.fileSystem;
    }

    public static boolean uploadExternalLibs(
            Path sourceLibsFolderPath, FileSystem sourceFileSystem,
            Path destinationLibsFolderPath, FileSystem destinationFileSystem
    ) throws IOException {
        if (!sourceFileSystem.isDirectory(sourceLibsFolderPath))
            return false;

        FileStatus[] files = sourceFileSystem.globStatus(sourceLibsFolderPath, jarFilesPathFilter);
        for (FileStatus file : files) {
            Path destinationJarFilePath = new Path(destinationLibsFolderPath, file.getPath().getName());
            destinationFileSystem.copyFromLocalFile(false, true, file.getPath(), destinationJarFilePath);
        }

        return true;
    }

    public void setExternalLibsPath(Path externalLibsPath) {
        this.externalLibsPath = externalLibsPath;
    }

    private void addJarFilesToJob(Job job) throws IOException {
        if (this.externalLibsPath == null || (!this.getFileSystem().exists(this.externalLibsPath)))
            return;

        FileStatus[] listOfFiles = this.getFileSystem().listStatus(this.externalLibsPath, jarFilesPathFilter);

        for (FileStatus file : listOfFiles) {
            job.addFileToClassPath(file.getPath());
        }
    }
}
