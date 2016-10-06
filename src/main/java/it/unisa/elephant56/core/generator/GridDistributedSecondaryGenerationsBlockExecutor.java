package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.core.reporter.individual.IndividualReporter;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.core.reporter.time.GeneticOperatorsTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;
import it.unisa.elephant56.util.common.Pair;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class GridDistributedSecondaryGenerationsBlockExecutor extends GenerationsBlockExecutor {

    private boolean isLastGeneration;

    public GridDistributedSecondaryGenerationsBlockExecutor(boolean isLastGeneration) {
        super();

        this.isLastGeneration = isLastGeneration;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run()
            throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        // Registers the generations block start time.
        long generationsBlockStartTime = System.currentTimeMillis();

        // Instantiates the classes.
        FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance = null;
        TerminationConditionCheck<Individual, FitnessValue> terminationConditionCheckClassInstance = null;
        Elitism<Individual, FitnessValue> elitismClassInstance = null;
        ParentsSelection<Individual, FitnessValue> parentsSelectionClassInstance = null;
        Crossover<Individual, FitnessValue> crossoverClassInstance = null;
        Mutation<Individual, FitnessValue> mutationClassInstance = null;
        SurvivalSelection<Individual, FitnessValue> survivalSelectionClassInstance = null;

        try {
            fitnessEvaluationClassInstance =
                    this.fitnessEvaluationClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            terminationConditionCheckClassInstance =
                    this.terminationConditionCheckClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            parentsSelectionClassInstance =
                    this.parentsSelectionClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            crossoverClassInstance =
                    this.crossoverClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            mutationClassInstance =
                    this.mutationClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);

            if (this.isSurvivalSelectionActive())
                survivalSelectionClassInstance =
                        this.survivalSelectionClass
                                .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                                .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                        this.configuration);

            if (this.isElitismActive())
                elitismClassInstance =
                        this.elitismClass
                                .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                                .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                        this.configuration);
        } catch (NullPointerException exception) {
            exception.printStackTrace();
            System.exit(-1);
        }

        // Instantiates the island properties.
        Properties islandProperties = new Properties();

        // Reads the input population and starts the generations.
        List<IndividualWrapper<Individual, FitnessValue>> currentPopulation = this.inputPopulation;

        this.currentGenerationNumber = this.startGenerationNumber;

        // Individuals termination condition check.
        this.runIndividualsTerminationConditionCheck(currentPopulation,
                terminationConditionCheckClassInstance);

        // Registers the generation start time.
        long generationStartTime = System.currentTimeMillis();

        // Checks if doing elitism.
        List<IndividualWrapper<Individual, FitnessValue>> elitePopulation = null;
        if (this.isElitismActive()) {
            // Elitism.
            elitePopulation =
                    this.runElitism(currentPopulation, elitismClassInstance);

            // Subtracts the elite from the current population.
            for (IndividualWrapper<Individual, FitnessValue> elitist : elitePopulation)
                currentPopulation.remove(elitist);
        }

        // Parents selection.
        List<Pair<IndividualWrapper<Individual, FitnessValue>,
                IndividualWrapper<Individual, FitnessValue>>> selectedCouples =
                this.runParentsSelection(currentPopulation, parentsSelectionClassInstance);

        // Crossover.
        List<IndividualWrapper<Individual, FitnessValue>> offspringPopulation =
                this.runCrossover(selectedCouples, crossoverClassInstance);

        // Mutation.
        this.runMutation(offspringPopulation, mutationClassInstance);

        // Checks if doing survival selection.
        if (this.isSurvivalSelectionActive()) {
            // Evaluates fitness values for the offspring.
            this.runOffspringFitnessEvaluation(offspringPopulation, fitnessEvaluationClassInstance);

            // Survival selection.
            currentPopulation =
                    this.runSurvivalSelection(currentPopulation, offspringPopulation,
                            survivalSelectionClassInstance, fitnessEvaluationClassInstance);
        } else {
            currentPopulation = offspringPopulation;
        }

        // Joins the elite population.
        if (this.isElitismActive())
            currentPopulation.addAll(elitePopulation);

        // Checks if it is the last generation and computes the fitness value for the population.
        if (this.isLastGeneration) {
            this.currentGenerationNumber += 1;

            // Fitness evaluation.
            this.runFitnessEvaluation(currentPopulation, fitnessEvaluationClassInstance);

            this.runIndividualsTerminationConditionCheck(currentPopulation,
                    terminationConditionCheckClassInstance);
        }

        // Sets the output population.
        this.outputPopulation = currentPopulation;

        // Sets the last executed generation number.
        this.lastExecutedGenerationNumber = this.currentGenerationNumber;
    }
}
