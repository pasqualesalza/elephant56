package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class GlobalDistributedGenerationsBlockExecutor extends GenerationsBlockExecutor {
    private FileSystem fileSystem;

    private String generationNameFormat;

    public GlobalDistributedGenerationsBlockExecutor(
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
        FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance = null;

        try {
            fitnessEvaluationClassInstance =
                    this.fitnessEvaluationClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
        } catch (NullPointerException exception) {
            exception.printStackTrace();
            System.exit(-1);
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
