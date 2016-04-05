package it.unisa.elephant56.core;

import it.unisa.elephant56.core.generator.*;
import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.input.NodesInputFormat;
import it.unisa.elephant56.core.reporter.time.MapReduceTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.operators.*;
import it.unisa.elephant56.user.operators.TerminationConditionCheck;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class IslandDistributedDriver extends DistributedDriver {

    // Driver objects.
    private int numberOfIslands;

    private Class<? extends Migration> migrationClass;
    private long migrationPeriod;

    /**
     * Constructs a new driver.
     *
     * @param applicationName the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     * @param numberOfIslands the number of islands
     */
    public IslandDistributedDriver(String applicationName, Class<?> applicationMainClass, int numberOfIslands) {
        super(applicationName, applicationMainClass);
        init(numberOfIslands);
    }

    /**
     * Constructs a new driver specifying the configuration,
     * referred only to the filesystem operations.
     *
     * @param applicationName the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     * @param numberOfIslands the number of islands
     * @param configuration the configuration
     */
    public IslandDistributedDriver(
            String applicationName, Class<?> applicationMainClass, int numberOfIslands, Configuration configuration) {
        super(applicationName, applicationMainClass, configuration);
        init(numberOfIslands);
    }

    /**
     * Initialises the driver.
     *
     * @param numberOfIslands the number of islands
     */
    private void init(int numberOfIslands) {
        this.numberOfIslands = numberOfIslands;

        this.migrationClass = Migration.class;
        this.migrationPeriod = 0L;
    }

    /**
     * Sets the migration class.
     *
     * @param migrationClass the migration class
     */
    public void setMigrationClass(Class<? extends Migration> migrationClass) {
        if (migrationClass == null)
            migrationClass = Migration.class;
        this.migrationClass = migrationClass;
    }

    /**
     * Sets the migration period.
     *
     * @param active how often migrating among the generations
     */
    public void setMigrationPeriod(long active) {
        this.migrationPeriod = active;
    }

    /**
     * Returns the termination flags folder path.
     *
     * @return the path
     */
    public Path getTerminationFlagsFolderPath() {
        return new Path(this.workingFolderPath, Constants.DEFAULT_TERMINATION_FLAG_FILES_FOLDER_NAME);
    }

    /**
     * Returns the islands properties folder path.
     *
     * @return the path
     */
    public Path getIslandsPropertiesFolderPath() {
        return new Path(this.workingFolderPath, Constants.DEFAULT_ISLANDS_PROPERTIES_FILES_FOLDER_NAME);
    }

    @Override
    public void run() throws Exception {
        // Creates the folders.
        this.createFolder(this.getGenerationsBlocksFolderPath(), true);
        this.createFolder(this.getTerminationFlagsFolderPath(), true);
        this.createFolder(this.getIslandsPropertiesFolderPath(), true);

        int initialisationPopulationSizePerSplit = (int) Math.ceil((double) this.initialisationPopulationSize / (double) this.numberOfIslands);

        // Copies the user properties into configuration.
        Configuration configurationWithUserProperties = new Configuration(this.configuration);
        this.userProperties.copyIntoConfiguration(configurationWithUserProperties);

        // Retrieves the schema.
        Schema individualWrapperSchema = IndividualWrapper.getSchema(this.individualClass, this.fitnessValueClass);

        // Computes the maximum number of jobs.
        long maximumNumberOfGenerationsBlocks;
        if ((this.maximumNumberOfGenerations == 0L) || (this.migrationPeriod < 1)) {
            maximumNumberOfGenerationsBlocks = 1L;
        } else {
            maximumNumberOfGenerationsBlocks =
                    (long) Math.ceil((double) this.maximumNumberOfGenerations / (double) this.migrationPeriod);
        }

        // Launches the jobs.
        long lastGenerationNumber = maximumNumberOfGenerations - 1L;
        long lastGenerationsBlockNumber = maximumNumberOfGenerationsBlocks - 1L;
        String generationNameFormat = getGenerationNameFormat(lastGenerationNumber);
        this.generationsBlockNameFormat = getGenerationsBlockNameFormat(lastGenerationsBlockNumber);
        long currentStartGenerationNumber = 0L;

        // TODO errore con migration period 0
        long currentFinishGenerationNumber = (this.migrationPeriod < 1 ? maximumNumberOfGenerations : this.migrationPeriod) - 1L;
        long previousGenerationsBlockNumber = -1L;
        long currentGenerationsBlockNumber = 0L;

        while (true) {
            // Checks if launching another job.
            if (currentGenerationsBlockNumber <= lastGenerationsBlockNumber) {
                // Checks if doing the migration.
                boolean activeMigration = true;
                if (currentGenerationsBlockNumber == (maximumNumberOfGenerationsBlocks - 1L)) {
                    activeMigration = false;
                }

                // Create the folder for the current generations block reports.
                Path currentGenerationsBlockReportsFolderPath = new Path(this.getReportsFolderPath(),
                        String.format(generationsBlockNameFormat, currentGenerationsBlockNumber));
                this.createFolder(currentGenerationsBlockReportsFolderPath, true);

                // Creates the job.
                Job currentGenerationsExecutorJob = this.createJob(
                        configurationWithUserProperties,
                        numberOfIslands,
                        initialisationPopulationSizePerSplit,
                        currentStartGenerationNumber,
                        currentFinishGenerationNumber,
                        previousGenerationsBlockNumber,
                        currentGenerationsBlockNumber,
                        generationNameFormat,
                        currentGenerationsBlockReportsFolderPath,
                        individualWrapperSchema,
                        activeMigration
                );

                // Creates the MapReduce time reporter.
                MapReduceTimeReporter mapreduceTimeReporter = null;
                if (this.isTimeReporterActive) {
                    // Creates and initialises the reporter.
                    Path mapreduceTimeReportFilePath = new Path(currentGenerationsBlockReportsFolderPath,
                            Constants.MAPREDUCE_TIME_REPORT_FILE_NAME_FORMAT);
                    mapreduceTimeReporter = new MapReduceTimeReporter(mapreduceTimeReportFilePath, this.fileSystem);
                    mapreduceTimeReporter.initialiseFile();

                    // Registers the mapper initialisation start partial time.
                    long startTime = System.currentTimeMillis();
                    for (int islandNumber = 0; islandNumber < this.numberOfIslands; islandNumber++)
                        mapreduceTimeReporter.addPartialTime(islandNumber, currentGenerationsBlockNumber,
                                MapReduceTimeReporter.PhaseType.MAPPER_INITIALISATION,
                                MapReduceTimeReporter.PartialTimeKey.Type.START, startTime);
                }

                // Launches the job.
                currentGenerationsExecutorJob.waitForCompletion(true);

                // Does some time reporter operations.
                if (this.isTimeReporterActive) {
                    // Registers the reducer finalisation finish partial time.
                    long finishTime = System.currentTimeMillis();
                    for (int islandNumber = 0; islandNumber < this.numberOfIslands; islandNumber++) {
                        mapreduceTimeReporter.addPartialTime(islandNumber, currentGenerationsBlockNumber,
                                MapReduceTimeReporter.PhaseType.REDUCER_FINALISATION,
                                MapReduceTimeReporter.PartialTimeKey.Type.FINISH, finishTime);

                        // Reads the partial times from mapper and reducer files.
                        Path mapreduceMapperPartialTimeReportFilePath = new Path(
                                currentGenerationsBlockReportsFolderPath,
                                String.format(Constants.MAPREDUCE_MAPPER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT,
                                        islandNumber)
                        );
                        mapreduceTimeReporter.addPartialTimesFromFile(mapreduceMapperPartialTimeReportFilePath,
                                this.getFileSystem());

                        Path mapreduceReducerPartialTimeReportFilePath = new Path(
                                currentGenerationsBlockReportsFolderPath,
                                String.format(Constants.MAPREDUCE_REDUCER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT,
                                        islandNumber)
                        );
                        mapreduceTimeReporter.addPartialTimesFromFile(mapreduceReducerPartialTimeReportFilePath,
                                this.getFileSystem());

                        // Joins the partial times.
                        mapreduceTimeReporter.joinPartialTimes(islandNumber, currentGenerationsBlockNumber);
                    }

                    // Writes the reporter file.
                    mapreduceTimeReporter.finaliseFile();
                }
            }

            // Checks if the maximum number of jobs has been reached.
            if (currentGenerationsBlockNumber == lastGenerationsBlockNumber)
                break;

            // Checks if it is time to stop for termination condition satisfaction.
            // TODO Riabilitare accesso HDFS.
            //if (DistributedGenerationsBlockExecutor.checkTerminationConditionSatisfaction(
            //        getTerminationFlagsFolderPath(), this.fileSystem))
            //    break;

            // Computes the start and finish generation numbers.
            currentStartGenerationNumber = currentFinishGenerationNumber + 1L;
            currentFinishGenerationNumber = currentStartGenerationNumber + this.migrationPeriod - 1L;

            previousGenerationsBlockNumber++;
            currentGenerationsBlockNumber++;
        }

        this.lastExecutedGenerationsBlockNumber = currentGenerationsBlockNumber;
    }

    protected Job createJob(
            Configuration configuration,
            int numberOfNodes,
            int initialisationPopulationSizePerSplit,
            long startGenerationNumber,
            long finishGenerationNumber,
            long previousGenerationsBlockNumber,
            long currentGenerationsBlockNumber,
            String generationNameFormat,
            Path currentGenerationsBlockReportsFolderPath,
            Schema individualWrapperSchema,
            boolean activeMigration
    ) throws IOException {
        // Creates a job.
        Job job = super.createJob(configuration, numberOfNodes, startGenerationNumber, finishGenerationNumber,
                previousGenerationsBlockNumber, currentGenerationsBlockNumber, generationNameFormat,
                currentGenerationsBlockReportsFolderPath, individualWrapperSchema,
                IslandMapper.class, SpecificNodePartitioner.class, IslandReducer.class);

        // Checks if is the first generations block.
        if (currentGenerationsBlockNumber == 0L) {
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
                    previousGenerationsBlockNumber, this.generationsBlockNameFormat));
            NodesInputFormat.activateInitialisation(job, false);
        }

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
        job.getConfiguration().set(Constants.CONFIGURATION_TERMINATION_FLAG_FILES_FOLDER_PATH,
                getTerminationFlagsFolderPath().toString());

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

        // Configures the Migration phase.
        if (activeMigration) {
            job.getConfiguration().setClass(Constants.CONFIGURATION_MIGRATION_CLASS, this.migrationClass,
                    Migration.class);
            job.getConfiguration().setBoolean(Constants.CONFIGURATION_MIGRATION_ACTIVE, true);
        }

        // Configures the island properties.
        job.getConfiguration().set(Constants.CONFIGURATION_ISLAND_PROPERTIES_FILES_FOLDER_PATH,
                this.getIslandsPropertiesFolderPath().toString());

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
