package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.FitnessEvaluation;
import it.unisa.elephant56.user.operators.Initialisation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class GridDistributedPrimaryGenerationsBlockExecutor extends GenerationsBlockExecutor {
    private FileSystem fileSystem;

    private String generationNameFormat;

    public GridDistributedPrimaryGenerationsBlockExecutor(
            FileSystem fileSystem,
            String generationNameFormat
    ) {
        super();

        this.fileSystem = fileSystem;

        this.generationNameFormat = generationNameFormat;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run()
            throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        // Instantiates the classes.
        Initialisation<Individual, FitnessValue> initalisationClassInstance = null;

        FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance = null;

        try {
            if (this.isInitialisationActive())
                initalisationClassInstance =
                        this.initiliasationClass
                                .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class,
                                        Integer.class)
                                .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                        this.configuration, this.initialisationPopulationSize);

            fitnessEvaluationClassInstance =
                    this.fitnessEvaluationClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
        } catch (NullPointerException exception) {
            exception.printStackTrace();
            System.exit(-1);
        }

        // Initialises the input population if needed.
        if (this.isInitialisationActive()) {
            long initialisationStartTime = System.currentTimeMillis();
            this.inputPopulation = runInitialisation(initalisationClassInstance);
            long initialisationFinishTime = System.currentTimeMillis();

            if (this.isTimeReporterActive())
                this.generationsBlockTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                        this.currentGenerationNumber, GenerationsBlockTimeReporter.PhaseType.INITIALISATION,
                        initialisationStartTime, initialisationFinishTime);
        }

        // Reads the input population and starts the generations.
        List<IndividualWrapper<Individual, FitnessValue>> currentPopulation = this.inputPopulation;

        this.currentGenerationNumber = this.startGenerationNumber;

        // Fitness evaluation.
        this.runFitnessEvaluation(currentPopulation, fitnessEvaluationClassInstance);

        // Sets the output population.
        this.outputPopulation = currentPopulation;

        // Sets the last executed generation number.
        this.lastExecutedGenerationNumber = this.currentGenerationNumber;
    }
}
