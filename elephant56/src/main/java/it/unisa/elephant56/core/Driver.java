package it.unisa.elephant56.core;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;
import it.unisa.elephant56.user.operators.Initialisation;
import it.unisa.elephant56.user.operators.TerminationConditionCheck;
import it.unisa.elephant56.util.hadoop.FilesPathFilter;
import org.apache.avro.Schema;
import org.apache.avro.file.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Initialises and runs a complete Genetic Algorithm execution.
 */
@SuppressWarnings("rawtypes")
public abstract class Driver {

    public static final FilesPathFilter avroFilesPathFilter = new FilesPathFilter(null, Constants.AVRO_FILE_EXTENSION);

    // Driver objects.
    protected Configuration configuration;

    protected FileSystem fileSystem;

    protected Class<? extends Individual> individualClass;
    protected Class<? extends FitnessValue> fitnessValueClass;

    protected Class<? extends Initialisation> initialisationClass;
    protected int initialisationPopulationSize;
    protected boolean isInitialisationActive;

    protected Class<? extends FitnessEvaluation> fitnessEvaluationClass;
    protected Class<? extends ParentsSelection> parentsSelectionClass;

    protected Class<? extends Elitism> elitismClass;
    protected boolean isElitismActive;

    protected Class<? extends Crossover> crossoverClass;
    protected Class<? extends Mutation> mutationClass;

    protected Class<? extends SurvivalSelection> survivalSelectionClass;
    protected boolean isSurvivalSelectionActive;

    protected Class<? extends TerminationConditionCheck> terminationConditionCheckClass;
    protected long maximumNumberOfGenerations;

    protected Path workingFolderPath;
    protected Path inputPopulationFolderPath;

    protected boolean isTimeReporterActive;
    protected Path timeReporterFolderPath;

    protected boolean isIndividualReporterActive;
    protected Path individualReporterFolderPath;

    protected Properties userProperties;

    /**
     * Constructs a new driver.
     */
    public Driver() {
        this.configuration = new Configuration();

        this.fitnessEvaluationClass = FitnessEvaluation.class;
        this.terminationConditionCheckClass = TerminationConditionCheck.class;

        this.elitismClass = Elitism.class;
        this.isElitismActive = false;

        this.parentsSelectionClass = ParentsSelection.class;
        this.crossoverClass = Crossover.class;
        this.mutationClass = Mutation.class;

        this.survivalSelectionClass = SurvivalSelection.class;
        this.isSurvivalSelectionActive = false;

        this.userProperties = new Properties();

        this.workingFolderPath = null;
        this.inputPopulationFolderPath = null;

        this.isTimeReporterActive = false;

        this.isIndividualReporterActive = false;
    }

    /**
     * Constructs a new driver.
     *
     * @param configuration the configuration
     */
    public Driver(Configuration configuration) {
        this();

        if (configuration != null)
            this.configuration = configuration;
    }

    /**
     * Sets the working directory path.
     *
     * @param path the working directory path
     */
    public void setWorkingFolderPath(Path path) {
        this.workingFolderPath = path;
    }

    public Path getWorkingFolderPath() {
        return this.workingFolderPath;
    }

    /**
     * Sets the input population folder. It is needed if the initialisation is not used.
     *
     * @param folderPath the input population folder path
     */
    public void setInputPopulationFolderPath(Path folderPath) {
        this.inputPopulationFolderPath = folderPath;
    }

    /**
     * Sets the individual class.
     *
     * @param individualClass the individual class
     */
    public void setIndividualClass(Class<? extends Individual> individualClass) {
        if (individualClass == null)
            individualClass = Individual.class;
        this.individualClass = individualClass;
    }

    /**
     * Sets the fitness value class.
     *
     * @param fitnessValueClass the fitness value class
     */
    public void setFitnessValueClass(Class<? extends FitnessValue> fitnessValueClass) {
        if (fitnessValueClass == null)
            fitnessValueClass = FitnessValue.class;
        this.fitnessValueClass = fitnessValueClass;

    }

    /**
     * Sets the initialisation class.
     *
     * @param initialisationClass the initialisation class
     */
    public void setInitialisationClass(Class<? extends Initialisation> initialisationClass) {
        if (initialisationClass == null)
            initialisationClass = Initialisation.class;
        this.initialisationClass = initialisationClass;
    }

    /**
     * Sets the number of individuals to generate during the initialisation.
     *
     * @param size the number of individuals to generate
     */
    public void setInitialisationPopulationSize(int size) {
        this.initialisationPopulationSize = size;
    }

    /**
     * Activates the initialisation. If not called,
     * the job finds the individuals inside the input paths.
     *
     * @param active "true" to activate, "false" otherwise
     */
    public void activateInitialisation(boolean active) {
        this.isInitialisationActive = active;
    }

    /**
     * Sets the fitness class.
     *
     * @param fitnessEvaluationClass the fitness class
     */
    public void setFitnessEvaluationClass(Class<? extends FitnessEvaluation> fitnessEvaluationClass) {
        if (fitnessEvaluationClass == null)
            fitnessEvaluationClass = FitnessEvaluation.class;
        this.fitnessEvaluationClass = fitnessEvaluationClass;
    }

    /**
     * Sets the elitism class to execute during the job.
     *
     * @param elitismClass the elitism class
     */
    public void setElitismClass(Class<? extends Elitism> elitismClass) {
        if (elitismClass == null)
            elitismClass = Elitism.class;
        this.elitismClass = elitismClass;
    }

    /**
     * Activates the elitism.
     *
     * @param active "true" to activate, "false" otherwise
     */
    public void activateElitism(boolean active) {
        this.isElitismActive = active;
    }

    /**
     * Sets the parents selection class.
     *
     * @param parentsSelectionClass the selection class
     */
    public void setParentsSelectionClass(Class<? extends ParentsSelection> parentsSelectionClass) {
        if (parentsSelectionClass == null)
            parentsSelectionClass = ParentsSelection.class;
        this.parentsSelectionClass = parentsSelectionClass;
    }

    /**
     * Sets the crossover class.
     *
     * @param crossoverClass the crossover class
     */
    public void setCrossoverClass(Class<? extends Crossover> crossoverClass) {
        if (crossoverClass == null)
            crossoverClass = Crossover.class;
        this.crossoverClass = crossoverClass;
    }

    /**
     * Sets the mutation class.
     *
     * @param mutationClass the mutation class
     */
    public void setMutationClass(Class<? extends Mutation> mutationClass) {
        if (mutationClass == null)
            mutationClass = Mutation.class;
        this.mutationClass = mutationClass;
    }

    /**
     * Sets the survival selection class.
     *
     * @param survivalSelectionClass the selection class
     */
    public void setSurvivalSelectionClass(Class<? extends SurvivalSelection> survivalSelectionClass) {
        if (survivalSelectionClass == null)
            survivalSelectionClass = SurvivalSelection.class;
        this.survivalSelectionClass = survivalSelectionClass;
    }

    /**
     * Activates the survival selection.
     *
     * @param active "true" to activate, "false" otherwise
     */
    public void activateSurvivalSelection(boolean active) {
        this.isSurvivalSelectionActive = active;
    }

    /**
     * Sets the termination condition check class.
     *
     * @param terminationConditionCheckClass the termination class
     */
    public void setTerminationConditionCheckClass(
            Class<? extends TerminationConditionCheck> terminationConditionCheckClass) {
        if (terminationConditionCheckClass == null)
            terminationConditionCheckClass = TerminationConditionCheck.class;
        this.terminationConditionCheckClass = terminationConditionCheckClass;
    }

    /**
     * Sets the maximum number of generations before terminating.
     *
     * @param maximumNumberOfGenerations the maximum number of generations
     */
    public void setMaximumNumberOfGenerations(long maximumNumberOfGenerations) {
        this.maximumNumberOfGenerations = maximumNumberOfGenerations;
    }

    /**
     * Sets the user properties.
     *
     * @param properties the user properties
     */
    public void setUserProperties(Properties properties) {
        if (properties == null)
            properties = new Properties();
        this.userProperties = properties;
    }

    /**
     * Activates the time reporter.
     *
     * @param active "true" to activate, "false" otherwise
     */
    public void activateTimeReporter(boolean active) {
        this.isTimeReporterActive = active;
    }

    /**
     * Activates the individual reporter.
     *
     * @param active "true" to activate, "false" otherwise
     */
    public void activateIndividualReporter(boolean active) {
        this.isIndividualReporterActive = active;
    }

    /**
     * Returns the reports folder path.
     *
     * @return the path
     */
    public Path getReportsFolderPath() {
        return new Path(this.workingFolderPath, Constants.DEFAULT_REPORTS_FOLDER_NAME);
    }

    public abstract List<IndividualWrapper<Individual, FitnessValue>> getOutputPopulation();

    public abstract List<IndividualWrapper<Individual, FitnessValue>> getSolutionsPopulation();

    public abstract List<IndividualWrapper<Individual, FitnessValue>> getNonsolutionsPopulation();

    public abstract long getLastExecutedGenerationNumber();

    public abstract void run() throws Exception;

    // It should be executed before anything else will be create inside the app.
    public void initialise() throws IOException {
        // Gets the file system.
        this.fileSystem = this.getFileSystem();

        // Creates the directories.
        this.createFolder(this.workingFolderPath, false);
        this.createFolder(this.getReportsFolderPath(), true);
    }

    public static List<IndividualWrapper<Individual, FitnessValue>> readIndividualsFromFile(
            Path filePath, Configuration configuration) throws IOException {
        List<IndividualWrapper<Individual, FitnessValue>> result =
                new ArrayList<IndividualWrapper<Individual, FitnessValue>>();

        SeekableInput seekableFileInput = new FsInput(filePath, configuration);
        ReflectData reflectData = new ReflectData(configuration.getClassLoader());
        DatumReader<IndividualWrapper<Individual, FitnessValue>> datumReader = new ReflectDatumReader<IndividualWrapper<Individual, FitnessValue>>(reflectData);
        DataFileReader<IndividualWrapper<Individual, FitnessValue>> avroFileReader =
                new DataFileReader<IndividualWrapper<Individual, FitnessValue>>(seekableFileInput, datumReader);

        for (IndividualWrapper<Individual, FitnessValue> individualWrapper : avroFileReader)
            result.add(individualWrapper);

        avroFileReader.close();
        return result;
    }

    public static List<IndividualWrapper<Individual, FitnessValue>>
    readIndividualsFromFolder(Path folderPath, Configuration configuration, PathFilter pathFilter)
            throws IOException {
        List<IndividualWrapper<Individual, FitnessValue>> result =
                new ArrayList<IndividualWrapper<Individual, FitnessValue>>();

        FileSystem fileSystem = FileSystem.get(configuration);
        FileStatus[] files = fileSystem.listStatus(folderPath, pathFilter);
        for (FileStatus file : files) {
            result.addAll(readIndividualsFromFile(file.getPath(), configuration));
        }

        return result;
    }

    public static void writeIndividualsToFile(
            Path filePath, Configuration configuration,
            List<IndividualWrapper<Individual, FitnessValue>> individuals,
            int startIndividualIndex, int numberOfIndividualsToWrite,
            Schema individualWrapperSchema) throws IOException {

        FileSystem fileSystem = FileSystem.get(configuration);

        OutputStream fileOutput = fileSystem.create(filePath, true);
        ReflectData reflectData = new ReflectData(configuration.getClassLoader());
        DatumWriter<IndividualWrapper<Individual, FitnessValue>> datumWriter = new ReflectDatumWriter<>(individualWrapperSchema, reflectData);
        DataFileWriter<IndividualWrapper<Individual, FitnessValue>> avroFileWriter =
                new DataFileWriter<>(datumWriter);

        avroFileWriter.setCodec(CodecFactory.snappyCodec());
        avroFileWriter.setMeta(Constants.AVRO_NUMBER_OF_RECORDS, numberOfIndividualsToWrite);

        avroFileWriter.create(individualWrapperSchema, fileOutput);

        int currentIndividualIndex = startIndividualIndex;
        for (int i = 0; i < numberOfIndividualsToWrite; i++) {
            IndividualWrapper<Individual, FitnessValue> currentIndividual = individuals.get(currentIndividualIndex);
            avroFileWriter.append(currentIndividual);
            currentIndividualIndex++;
        }

        avroFileWriter.close();
    }

    public static void
    writeIndividualsToFolder(Path folderPath, Configuration configuration,
                             List<IndividualWrapper<Individual, FitnessValue>> individuals,
                             Schema individualWrapperSchema,
                             String fileNameFormat, int totalNumberOfFiles)
            throws IOException {

        int numberOfIndividualsPerFile = (int) Math.ceil((double) individuals.size() / (double) totalNumberOfFiles);

        int currentStartIndividualIndex = 0;
        for (int currentFileNumber = 0; currentFileNumber < totalNumberOfFiles; currentFileNumber++) {
            Path currentFilePath = new Path(folderPath,
                    String.format(fileNameFormat, currentFileNumber));

            int currentNumberOfIndividualsToWrite = numberOfIndividualsPerFile;
            if (((currentFileNumber + 1) * numberOfIndividualsPerFile) > individuals.size())
                currentNumberOfIndividualsToWrite = individuals.size() - (currentFileNumber * numberOfIndividualsPerFile);

            writeIndividualsToFile(currentFilePath, configuration, individuals, currentStartIndividualIndex, currentNumberOfIndividualsToWrite, individualWrapperSchema);

            currentStartIndividualIndex += numberOfIndividualsPerFile;
        }
    }

    public abstract FileSystem getFileSystem() throws IOException;

    public void createFolder(Path folderPath, boolean overwrite) throws IOException {
        if (this.fileSystem.exists(folderPath) && overwrite)
            this.fileSystem.delete(folderPath, overwrite);
        this.fileSystem.mkdirs(folderPath);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
