package it.unisa.elephant56.core;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.input.NodesInputFormat;
import it.unisa.elephant56.core.output.NodesOutputFormat;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.util.hadoop.AvroKeyAllDifferentComparator;
import it.unisa.elephant56.util.hadoop.AvroKeyAllEqualComparator;
import it.unisa.elephant56.util.hadoop.FilesPathFilter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DistributedDriver extends Driver {

    public final static FilesPathFilter jarFilesPathFilter = new FilesPathFilter(null, Constants.JAR_FILE_EXTENSION);
    public final static FilesPathFilter populationFilesPathFilter =
            new FilesPathFilter(Constants.CONFIGURATION_POPULATION_NAME, Constants.AVRO_FILE_EXTENSION);

    // Driver objects.
    protected String applicationName;
    protected Class<?> applicationMainClass;

    // TODO Aggiungere metodo di controllo.
    protected boolean keepGenerationsFolderWhenComplete;

    protected boolean isCompressionActive;

    protected long lastExecutedGenerationNumber;
    protected long lastExecutedGenerationsBlockNumber;

    protected String generationsBlockNameFormat;

    protected Path outputFolderPath;

    public Path externalLibsPath;

    /**
     * Constructs a new driver.
     *
     * @param applicationName      the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     */
    public DistributedDriver(String applicationName, Class<?> applicationMainClass) {
        super();
        init(applicationName, applicationMainClass);
    }

    /**
     * Constructs a new driver specifying the configuration,
     * referred only to the filesystem operations.
     *
     * @param applicationName      the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     * @param configuration        the configuration
     */
    public DistributedDriver(
            String applicationName, Class<?> applicationMainClass, Configuration configuration) {
        super(configuration);
        init(applicationName, applicationMainClass);
    }

    /**
     * Initialises the driver.
     *
     * @param applicationName      the name of the application
     * @param applicationMainClass the main class that launches the chain on the cluster
     */
    private void init(String applicationName, Class<?> applicationMainClass) {
        this.applicationName = applicationName;
        this.applicationMainClass = applicationMainClass;

        this.keepGenerationsFolderWhenComplete = false;

        this.isCompressionActive = false;
    }

    /**
     * Defines if keeping or not the generations folder after the whole work ends.
     *
     * @param keep "true" to keep, "false" otherwise
     */
    public void setKeepGenerationsFolderWhenComplete(boolean keep) {
        this.keepGenerationsFolderWhenComplete = keep;
    }

    /**
     * Activates the compression of individuals.
     *
     * @param active "true" to activate, "false" otherwise
     */
    public void activateCompression(boolean active) {
        this.isCompressionActive = active;
    }

    /**
     * Returns the output folder path.
     *
     * @return the path
     */
    public Path getOutputFolderPath() {
        return this.getGenerationsBlockFolderPath(
                this.getLastExecutedGenerationsBlockNumber(), this.generationsBlockNameFormat);
    }

    /**
     * Returns the generations blocks folder path.
     *
     * @return the path
     */
    public Path getGenerationsBlocksFolderPath() {
        return new Path(this.workingFolderPath, Constants.DEFAULT_GENERATIONS_BLOCKS_FOLDER_NAME);
    }

    public Path getGenerationsBlockFolderPath(long generationsBlockNumber, String generationsBlockNameFormat) {
        return new Path(getGenerationsBlocksFolderPath(),
                String.format(generationsBlockNameFormat, generationsBlockNumber));
    }

    @Override
    public List<IndividualWrapper<Individual, FitnessValue>> getOutputPopulation() {
        List<IndividualWrapper<Individual, FitnessValue>> outputPopulation;
        try {
            outputPopulation =
                    readIndividualsFromFolder(this.getOutputFolderPath(), this.configuration,
                            populationFilesPathFilter);
        } catch (IOException exception) {
            return new ArrayList<IndividualWrapper<Individual, FitnessValue>>(0);
        }

        return outputPopulation;
    }

    // TODO Implementare getSolutionsPopulation().
    @Override
    public List<IndividualWrapper<Individual, FitnessValue>> getSolutionsPopulation() {
        return null;
    }

    // TODO Implementare getNonsolutionsPopulation().
    @Override
    public List<IndividualWrapper<Individual, FitnessValue>> getNonsolutionsPopulation() {
        return null;
    }

    // TODO Implementare getLastExecutedGenerationNumber(), deve dare il numero massimo tra le isole.
    @Override
    public long getLastExecutedGenerationNumber() {
        return this.maximumNumberOfGenerations - 1L;
    }

    public long getLastExecutedGenerationsBlockNumber() {
        return this.lastExecutedGenerationsBlockNumber;
    }

    @Override
    public void run() throws Exception {
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

    protected String getGenerationNameFormat(long generationNumber) {
        return "%0" + Long.toString(generationNumber).length() + "d";
    }

    protected String getGenerationsBlockNameFormat(long generationsBlockNumber) {
        return "%0" + Long.toString(generationsBlockNumber).length() + "d";
    }

    protected Job createJob(
            Configuration configuration,
            int numberOfNodes,
            long startGenerationNumber,
            long finishGenerationNumber,
            long previousGenerationsBlockNumber,
            long currentGenerationsBlockNumber,
            String generationNameFormat,
            Path currentGenerationsBlockReportsFolderPath,
            Schema individualWrapperSchema,
            Class<? extends Mapper> mapperClass,
            Class<? extends Partitioner> partitionerClass,
            Class<? extends Reducer> reducerClass
    ) throws IOException {

        // Copies the configuration.
        Configuration configurationCopy = new Configuration(configuration);

        // Creates a job.
        Job job = Job.getInstance(configurationCopy);

        // Sets the job name.
        job.setJobName(this.applicationName);

        // Configures the input format.
        job.setInputFormatClass(NodesInputFormat.class);
        AvroJob.setInputKeySchema(job, individualWrapperSchema);

        NodesInputFormat.setNumberOfNodes(job, numberOfNodes);

        // Configures some properties.
        job.getConfiguration().setLong(Constants.CONFIGURATION_GENERATIONS_BLOCK_NUMBER, currentGenerationsBlockNumber);
        job.getConfiguration().set(Constants.CONFIGURATION_GENERATION_NAME_FORMAT, generationNameFormat);

        // Configures the mapper phase.
        job.setMapperClass(mapperClass);

        // Configures the generations limits.
        job.getConfiguration().setLong(Constants.CONFIGURATION_START_GENERATION_NUMBER, startGenerationNumber);
        job.getConfiguration().setLong(Constants.CONFIGURATION_FINISH_GENERATION_NUMBER, finishGenerationNumber);

        // Configures the mapper output.
        AvroJob.setMapOutputKeySchema(job, individualWrapperSchema);
        job.setMapOutputValueClass(IntWritable.class);

        // Configures the partitioner.
        job.setPartitionerClass(partitionerClass);

        // Configures the grouping and sorting comparators.
        job.setGroupingComparatorClass(AvroKeyAllDifferentComparator.class);
        job.setSortComparatorClass(AvroKeyAllEqualComparator.class);

        // Configures the reducer phase.
        job.setNumReduceTasks(numberOfNodes);
        job.setReducerClass(reducerClass);

        // Configures the output format.
        job.setOutputFormatClass(NodesOutputFormat.class);

        job.getConfiguration().set("avro.mo.config.namedOutput", Constants.CONFIGURATION_POPULATION_NAME);

        AvroJob.setOutputKeySchema(job, individualWrapperSchema);

        AvroMultipleOutputs.addNamedOutput(job, Constants.CONFIGURATION_SOLUTIONS_NAME, AvroKeyOutputFormat.class,
                individualWrapperSchema);
        AvroMultipleOutputs.addNamedOutput(job, Constants.CONFIGURATION_NONSOLUTIONS_NAME, AvroKeyOutputFormat.class,
                individualWrapperSchema);

        NodesOutputFormat.setOutputPath(job, this.getGenerationsBlockFolderPath(currentGenerationsBlockNumber,
                this.generationsBlockNameFormat));

        // Configures the reporter folder.
        job.getConfiguration().set(Constants.CONFIGURATION_REPORTS_FOLDER_PATH,
                currentGenerationsBlockReportsFolderPath.toString());

        // Configures the time reporter.
        if (this.isTimeReporterActive) {
            job.getConfiguration().setBoolean(Constants.CONFIGURATION_TIME_REPORTER_ACTIVE, true);
        }

        // Configures the individual reporter.
        if (this.isIndividualReporterActive) {
            job.getConfiguration().setBoolean(Constants.CONFIGURATION_INDIVIDUAL_REPORTER_ACTIVE, true);
        }

        // Sets the Snappy compression.
        if (this.isCompressionActive) {
            job.getConfiguration().setBoolean("mapred.output.compress", true);
            job.getConfiguration().set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
        }

        // Sets the jar.
        job.setJarByClass(this.applicationMainClass);

        // Returns the job.
        return job;
    }
}
