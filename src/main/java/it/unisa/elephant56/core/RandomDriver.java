package it.unisa.elephant56.core;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.generator.GenerationsBlockExecutor;
import it.unisa.elephant56.core.generator.RandomGenerationsBlockExecutor;
import it.unisa.elephant56.core.reporter.individual.IndividualReporter;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.core.reporter.time.GeneticOperatorsTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RandomDriver extends Driver {

    // Driver objects.
    private List<IndividualWrapper<Individual, FitnessValue>> inputPopulation;
    private List<IndividualWrapper<Individual, FitnessValue>> outputPopulation;
    private List<IndividualWrapper<Individual, FitnessValue>> solutionsPopulation;
    private List<IndividualWrapper<Individual, FitnessValue>> nonsolutionsPopulation;

    /**
     * Constructs a new driver.
     */
    public RandomDriver() {
        super();
        
        this.inputPopulation = new ArrayList<IndividualWrapper<Individual, FitnessValue>>();
    }

    public void setInputPopulation(List<IndividualWrapper<Individual, FitnessValue>> inputPopulation) {
        if (inputPopulation != null)
            this.inputPopulation = inputPopulation;
    }

    @Override
    public void run() throws Exception {
        // Copies the user properties into configuration.
        Configuration finalConfiguration = new Configuration(this.configuration);
        this.userProperties.copyIntoConfiguration(finalConfiguration);

        // Creates and configures the only generation block executor.
        GenerationsBlockExecutor generationsBlockExecutor = new RandomGenerationsBlockExecutor();

        generationsBlockExecutor.setConfiguration(this.configuration);
        generationsBlockExecutor.setUserProperties(this.userProperties);

        generationsBlockExecutor.setNodeNumber(0);
        generationsBlockExecutor.setTotalNumberOfNodes(1);

        generationsBlockExecutor.setGenerationsBlockNumber(0L);

        generationsBlockExecutor.setStartGenerationNumber(0L);
        generationsBlockExecutor.setFinishGenerationNumber(0L);

        generationsBlockExecutor.setInitiliasationClass(this.initialisationClass);
        generationsBlockExecutor.setFitnessEvaluationClass(this.fitnessEvaluationClass);
        generationsBlockExecutor.setTerminationConditionCheckClass(this.terminationConditionCheckClass);

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

    @Override
    public List<IndividualWrapper<Individual, FitnessValue>> getOutputPopulation() {
        return this.outputPopulation;
    }

    @Override
    public List<IndividualWrapper<Individual, FitnessValue>> getSolutionsPopulation() {
        return this.solutionsPopulation;
    }

    @Override
    public List<IndividualWrapper<Individual, FitnessValue>> getNonsolutionsPopulation() {
        return this.nonsolutionsPopulation;
    }

    @Override
    public long getLastExecutedGenerationNumber() {
        return 0L;
    }

    public FileSystem getFileSystem() throws IOException {
        if (this.fileSystem == null)
            this.fileSystem = FileSystem.getLocal(this.configuration);
        return this.fileSystem;
    }
}
