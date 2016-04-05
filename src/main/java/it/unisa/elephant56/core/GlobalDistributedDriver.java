package it.unisa.elephant56.core;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.generator.*;
import it.unisa.elephant56.core.input.NodesInputFormat;
import it.unisa.elephant56.core.output.NodesOutputFormat;
import it.unisa.elephant56.core.reporter.individual.IndividualReporter;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.core.reporter.time.GeneticOperatorsTimeReporter;
import it.unisa.elephant56.core.reporter.time.MapReduceTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GlobalDistributedDriver extends DistributedDriver {

    public static enum FitnessEvaluationType {
        PARENTS,
        OFFSPRING
    }

    // Driver objects.
    private int numberOfNodes;

    private List<IndividualWrapper<Individual, FitnessValue>> inputPopulation;
    private List<IndividualWrapper<Individual, FitnessValue>> outputPopulation;
    private List<IndividualWrapper<Individual, FitnessValue>> solutionsPopulation;
    private List<IndividualWrapper<Individual, FitnessValue>> nonsolutionsPopulation;

    private Schema individualWrapperSchema;

    GlobalGenerationsBlockExecutor generationsBlockExecutor;

    /**
     * Constructs a new driver.
     *
     * @param applicationName the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     * @param numberOfNodes the number of fitness evaluators
     */
    public GlobalDistributedDriver(String applicationName, Class<?> applicationMainClass, int numberOfNodes) {
        super(applicationName, applicationMainClass);
        init(numberOfNodes);
    }

    /**
     * Constructs a new driver specifying the configuration,
     * referred only to the filesystem operations.
     *
     * @param applicationName the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     * @param numberOfNodes the number of fitness evaluators
     * @param configuration the configuration
     */
    public GlobalDistributedDriver(
            String applicationName, Class<?> applicationMainClass, int numberOfNodes, Configuration configuration) {
        super(applicationName, applicationMainClass, configuration);
        init(numberOfNodes);
    }

    /**
     * Initialises the driver.
     *
     * @param numberOfNodes the number of nodes
     */
    private void init(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;

        this.inputPopulation = new ArrayList<IndividualWrapper<Individual, FitnessValue>>();
    }

    public void setInputPopulation(List<IndividualWrapper<Individual, FitnessValue>> inputPopulation) {
        if (inputPopulation != null)
            this.inputPopulation = inputPopulation;
    }

    @Override
    public void run() throws Exception {
        // Creates the folders.
        this.createFolder(this.getGenerationsBlocksFolderPath(), true);

        // Retrieves the schema.
        this.individualWrapperSchema = IndividualWrapper.getSchema(this.individualClass, this.fitnessValueClass);

        // Creates and configures the only generation block executor.
        this.generationsBlockExecutor = new GlobalGenerationsBlockExecutor(this);

        generationsBlockExecutor.setConfiguration(this.configuration);
        generationsBlockExecutor.setUserProperties(this.userProperties);

        generationsBlockExecutor.setNodeNumber(0);
        generationsBlockExecutor.setTotalNumberOfNodes(1);

        generationsBlockExecutor.setGenerationsBlockNumber(0L);

        generationsBlockExecutor.setStartGenerationNumber(0L);
        long lastGenerationNumber = this.maximumNumberOfGenerations - 1L;
        generationsBlockExecutor.setFinishGenerationNumber(lastGenerationNumber);

        generationsBlockExecutor.setInitiliasationClass(this.initialisationClass);
        generationsBlockExecutor.setFitnessEvaluationClass(this.fitnessEvaluationClass);
        generationsBlockExecutor.setTerminationConditionCheckClass(this.terminationConditionCheckClass);

        generationsBlockExecutor.setElitismClass(this.elitismClass);
        generationsBlockExecutor.activateElitism(this.isElitismActive);

        generationsBlockExecutor.setParentsSelectionClass(this.parentsSelectionClass);
        generationsBlockExecutor.setCrossoverClass(this.crossoverClass);
        generationsBlockExecutor.setMutationClass(this.mutationClass);

        generationsBlockExecutor.setSurvivalSelectionClass(this.survivalSelectionClass);
        generationsBlockExecutor.activateSurvivalSelection(this.isSurvivalSelectionActive);

        // Checks if reading individual or initialising them.
        if (!this.isInitialisationActive) {
            generationsBlockExecutor.activateInitialisation(false);
            this.inputPopulation.addAll(this.readIndividualsFromFolder(this.inputPopulationFolderPath,
                    this.configuration, avroFilesPathFilter));
            generationsBlockExecutor.setInputPopulation(this.inputPopulation);
        } else {
            generationsBlockExecutor.activateInitialisation(true);
            generationsBlockExecutor.setInitialisationPopulationSize(this.initialisationPopulationSize);
        }

        // Configures the reporters.
        GeneticOperatorsTimeReporter geneticOperatorsTimeReporter = null;
        GenerationsBlockTimeReporter generationsBlockTimeReporter = null;

        if (this.isTimeReporterActive) {
            Path geneticOperatorsTimeReportFilePath = new Path(this.getReportsFolderPath(),
                    String.format(Constants.GENETIC_OPERATORS_TIME_REPORT_FILE_NAME_FORMAT, 0));
            geneticOperatorsTimeReporter = new GeneticOperatorsTimeReporter(geneticOperatorsTimeReportFilePath,
                    this.fileSystem);

            geneticOperatorsTimeReporter.initialiseFile();
            generationsBlockExecutor.setGeneticOperatorsTimeReporter(geneticOperatorsTimeReporter);

            Path generationsBlockTimeReportFilePath = new Path(this.getReportsFolderPath(),
                    String.format(Constants.GENERATIONS_BLOCK_TIME_REPORT_FILE_NAME_FORMAT, 0));
            generationsBlockTimeReporter =
                    new GenerationsBlockTimeReporter(generationsBlockTimeReportFilePath, this.fileSystem);

            generationsBlockTimeReporter.initialiseFile();

            generationsBlockExecutor.setGenerationsBlockTimeReporter(generationsBlockTimeReporter);
            generationsBlockExecutor.activateTimeReporter(true);
        }

        IndividualReporter individualReporter = null;

        if (this.isIndividualReporterActive) {
            Path individualReportFilePath = new Path(this.getReportsFolderPath(),
                    String.format(Constants.INDIVIDUAL_REPORT_FILE_NAME_FORMAT, 0));
            individualReporter = new IndividualReporter(individualReportFilePath, this.fileSystem);

            individualReporter.initialiseFile();
            generationsBlockExecutor.setIndividualReporter(individualReporter);

            generationsBlockExecutor.activateIndividualReporter(true);
        }

        // Runs the generations block executor.
        generationsBlockExecutor.run();

        this.lastExecutedGenerationNumber = getLastExecutedGenerationNumber();

        // Finalises the reports.
        if (this.isTimeReporterActive) {
            geneticOperatorsTimeReporter.finaliseFile();
            generationsBlockTimeReporter.finaliseFile();
        }
        if (this.isIndividualReporterActive) {
            individualReporter.finaliseFile();
        }

        generationsBlockExecutor.filterSolutions();

        // Retrieves the output populations.
        this.outputPopulation = generationsBlockExecutor.getOutputPopulation();
        this.solutionsPopulation = generationsBlockExecutor.getSolutionsPopulation();
        this.nonsolutionsPopulation = generationsBlockExecutor.getNonsolutionsPopulation();
    }

    public Job createJob(
            Configuration configuration,
            int numberOfNodes,
            long currentGenerationNumber,
            String generationNameFormat,
            Path currentGenerationsBlockReportsFolderPath,
            Schema individualWrapperSchema
    ) throws IOException {
        // Creates a job.
        Job job = super.createJob(configuration, numberOfNodes, currentGenerationNumber, currentGenerationNumber,
                (currentGenerationNumber - 1L), currentGenerationNumber, generationNameFormat,
                currentGenerationsBlockReportsFolderPath, individualWrapperSchema,
                GlobalMapper.class, Partitioner.class, Reducer.class);

        // Sets the input.
        NodesInputFormat.setInputPopulationFolderPath(job, this.getInputFolderPath());
        NodesInputFormat.activateInitialisation(job, false);

        // Configures the fitness value class.
        job.getConfiguration().setClass(Constants.CONFIGURATION_FITNESS_VALUE_CLASS, this.fitnessValueClass,
                FitnessValue.class);

        // Configures the Fitness Evaluation phase.
        job.getConfiguration().setClass(Constants.CONFIGURATION_FITNESS_EVALUATION_CLASS, this.fitnessEvaluationClass,
                FitnessEvaluation.class);

        // Disables the reducer.
        job.setNumReduceTasks(0);

        // Returns the job.
        return job;
    }

    public void runDistributedFitnessEvaluation(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            FitnessEvaluationType fitnessEvaluationType
    ) throws Exception {
        // Copies the user properties into configuration.
        Configuration configurationWithUserProperties = new Configuration(this.configuration);
        this.userProperties.copyIntoConfiguration(configurationWithUserProperties);

        // Retrieves the schema.
        Schema individualWrapperSchema = IndividualWrapper.getSchema(this.individualClass, this.fitnessValueClass);

        // Launches the jobs.
        long lastGenerationNumber = maximumNumberOfGenerations - 1L;
        String generationNameFormat = getGenerationNameFormat(lastGenerationNumber);
        this.generationsBlockNameFormat = getGenerationsBlockNameFormat(lastGenerationNumber);
        long currentGenerationNumber = this.generationsBlockExecutor.getCurrentGenerationNumber();

        // Create the folder for the current generations block reports.
        Path currentGenerationsBlockReportsFolderPath = new Path(this.getReportsFolderPath(),
                String.format(generationsBlockNameFormat, currentGenerationNumber));
        if (fitnessEvaluationType == FitnessEvaluationType.PARENTS) {
            currentGenerationsBlockReportsFolderPath = new Path(currentGenerationsBlockReportsFolderPath, Constants.DEFAULT_PARENTS_OUTPUT_FOLDER_NAME);
        } else {
            currentGenerationsBlockReportsFolderPath = new Path(currentGenerationsBlockReportsFolderPath, Constants.DEFAULT_OFFSPRING_OUTPUT_FOLDER_NAME);
        }
        this.createFolder(currentGenerationsBlockReportsFolderPath, true);

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
            for (int islandNumber = 0; islandNumber < this.numberOfNodes; islandNumber++)
                mapreduceTimeReporter.addPartialTime(islandNumber, currentGenerationNumber,
                        MapReduceTimeReporter.PhaseType.MAPPER_INITIALISATION,
                        MapReduceTimeReporter.PartialTimeKey.Type.START, startTime);
        }

        // Creates the folders.
        this.createFolder(this.getInputFolderPath(), true);

        // Writes the population into HDFS.
        String fileNameFormat = generationNameFormat + "." + Constants.AVRO_FILE_EXTENSION;
        this.writeIndividualsToFolder(this.getInputFolderPath(), getConfiguration(), population,
                this.individualWrapperSchema, fileNameFormat, numberOfNodes);

        // Creates the job.
        Job currentGenerationsExecutorJob = this.createJob(
                configurationWithUserProperties,
                numberOfNodes,
                currentGenerationNumber,
                generationNameFormat,
                currentGenerationsBlockReportsFolderPath,
                individualWrapperSchema
        );

        // Modifies the output folder.
        Path oldOutputFolderPath = NodesOutputFormat.getOutputPath(currentGenerationsExecutorJob);
        Path newOutputFolderPath;
        if (fitnessEvaluationType == FitnessEvaluationType.PARENTS) {
            newOutputFolderPath = new Path(oldOutputFolderPath, Constants.DEFAULT_PARENTS_OUTPUT_FOLDER_NAME);
        } else {
            newOutputFolderPath = new Path(oldOutputFolderPath, Constants.DEFAULT_OFFSPRING_OUTPUT_FOLDER_NAME);
        }
        NodesOutputFormat.setOutputPath(currentGenerationsExecutorJob, newOutputFolderPath);

        // Launches the job.
        currentGenerationsExecutorJob.waitForCompletion(true);

        // Does some time reporter operations.
        if (this.isTimeReporterActive) {
            // Registers the reducer finalisation finish partial time.
            long finishTime = System.currentTimeMillis();
            for (int fitnessEvaluatorNumber = 0; fitnessEvaluatorNumber < this.numberOfNodes; fitnessEvaluatorNumber++) {
                mapreduceTimeReporter.addPartialTime(fitnessEvaluatorNumber, currentGenerationNumber,
                        MapReduceTimeReporter.PhaseType.MAPPER_FINALISATION,
                        MapReduceTimeReporter.PartialTimeKey.Type.FINISH, finishTime);

                // Reads the partial times from mapper and reducer files.
                Path mapreduceMapperPartialTimeReportFilePath = new Path(
                        currentGenerationsBlockReportsFolderPath,
                        String.format(Constants.MAPREDUCE_MAPPER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT,
                                fitnessEvaluatorNumber)
                );
                mapreduceTimeReporter.addPartialTimesFromFile(mapreduceMapperPartialTimeReportFilePath,
                        this.getFileSystem());

                Path mapreduceReducerPartialTimeReportFilePath = new Path(
                        currentGenerationsBlockReportsFolderPath,
                        String.format(Constants.MAPREDUCE_MAPPER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT,
                                fitnessEvaluatorNumber)
                );
                mapreduceTimeReporter.addPartialTimesFromFile(mapreduceReducerPartialTimeReportFilePath,
                        this.getFileSystem());

                // Joins the partial times.
                mapreduceTimeReporter.joinPartialTimes(fitnessEvaluatorNumber, currentGenerationNumber);
            }

            // Writes the reporter file.
            mapreduceTimeReporter.finaliseFile();

            this.lastExecutedGenerationsBlockNumber = currentGenerationNumber;
        }
    }

    public Path getInputFolderPath() {
        return new Path(this.workingFolderPath, Constants.DEFAULT_INPUT_FOLDER_NAME);
    }

    public List<IndividualWrapper<Individual, FitnessValue>> getResultPopulation(
            FitnessEvaluationType fitnessEvaluationType
    ) {
        List<IndividualWrapper<Individual, FitnessValue>> outputPopulation;
        try {
            Path outputFolderPath;
            if (fitnessEvaluationType == FitnessEvaluationType.PARENTS) {
                outputFolderPath = new Path(this.getOutputFolderPath(), Constants.DEFAULT_PARENTS_OUTPUT_FOLDER_NAME);
            } else {
                outputFolderPath = new Path(this.getOutputFolderPath(), Constants.DEFAULT_OFFSPRING_OUTPUT_FOLDER_NAME);
            }

            outputPopulation =
                    readIndividualsFromFolder(outputFolderPath, this.configuration,
                            populationFilesPathFilter);
        } catch (IOException exception) {
            return new ArrayList<IndividualWrapper<Individual, FitnessValue>>(0);
        }

        return outputPopulation;
    }

    @Override
    public List<IndividualWrapper<Individual, FitnessValue>> getOutputPopulation() {
        return this.outputPopulation;
    }
}
