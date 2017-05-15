package it.unisa.elephant56.onemax;

import org.apache.hadoop.fs.Path;

import it.unisa.elephant56.core.Driver;
import it.unisa.elephant56.core.SequentialDriver;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.BitStringIndividual;
import it.unisa.elephant56.user.operators.BitStringMutation;
import it.unisa.elephant56.user.operators.BitStringSinglePointCrossover;
import it.unisa.elephant56.user.operators.OneMaxFitnessEvaluation;
import it.unisa.elephant56.user.operators.RandomBitStringInitialisation;
import it.unisa.elephant56.user.sample.common.fitness_value.IntegerFitnessValue;
import it.unisa.elephant56.user.sample.operators.elitism.BestIndividualsElitism;
import it.unisa.elephant56.user.sample.operators.parents_selection.RouletteWheelParentsSelection;
import it.unisa.elephant56.user.sample.operators.survival_selection.RouletteWheelSurvivalSelection;

public class SequentialApp {
    private static final long RANDOM_SEED = 0;
    private static final int POPULATION_SIZE = 1000;
    private static final int INDIVIDUAL_SIZE = 100;
    private static final int NUMBER_OF_ELITISTS = 5;
    private static final double MUTATION_PROBABILITY = 0.2;
    private static final long MAXIMUM_NUMBER_OF_GENERATIONS = 100;

    private static final String WORKING_DIRECTORY = "onemax";

    public static void main(String[] args) throws Exception {
        // Creates the user properties.
        Properties userProperties = new Properties();

        // Creates the driver.
        Driver driver = new SequentialDriver();

        // Configures the working directory.
        Path workingFolderPath = new Path(WORKING_DIRECTORY);
        driver.setWorkingFolderPath(workingFolderPath);

        // Sets the properties.
        driver.setUserProperties(userProperties);

        // Initialises the driver.
        driver.initialise();

        // Individual and fitness value configuration.
        driver.setIndividualClass(BitStringIndividual.class);
        driver.setFitnessValueClass(IntegerFitnessValue.class);

        // Initialisation configuration.
        driver.activateInitialisation(true);
        driver.setInitialisationClass(RandomBitStringInitialisation.class);
        driver.setInitialisationPopulationSize(POPULATION_SIZE);
        userProperties.setInt(RandomBitStringInitialisation.INT_INDIVIDUAL_SIZE, INDIVIDUAL_SIZE);
        userProperties.setLong(RandomBitStringInitialisation.LONG_RANDOM_SEED, RANDOM_SEED);

        // Fitness evaluation configuration.
        driver.setFitnessEvaluationClass(OneMaxFitnessEvaluation.class);

        // Elitism configuration.
        driver.setElitismClass(BestIndividualsElitism.class);
        driver.activateElitism(true);
        userProperties.setInt(BestIndividualsElitism.INT_NUMBER_OF_ELITISTS, NUMBER_OF_ELITISTS);

        // Parents selection configuration.
        driver.setParentsSelectionClass(RouletteWheelParentsSelection.class);
        userProperties.setBoolean(RouletteWheelParentsSelection.BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED, true);
        userProperties.setLong(RouletteWheelParentsSelection.LONG_RANDOM_SEED, RANDOM_SEED);

        // Crossover configuration.
        driver.setCrossoverClass(BitStringSinglePointCrossover.class);
        userProperties.setLong(BitStringSinglePointCrossover.LONG_RANDOM_SEED, RANDOM_SEED);

        // Mutation configuration.
        driver.setMutationClass(BitStringMutation.class);
        userProperties.setDouble(BitStringMutation.DOUBLE_PROBABILITY, MUTATION_PROBABILITY);
        userProperties.setLong(BitStringMutation.LONG_RANDOM_SEED, RANDOM_SEED);

        // Survival selection configuration.
        driver.setSurvivalSelectionClass(RouletteWheelSurvivalSelection.class);
        driver.activateSurvivalSelection(true);
        userProperties.setBoolean(RouletteWheelSurvivalSelection.BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED, true);
        userProperties.setLong(RouletteWheelSurvivalSelection.LONG_RANDOM_SEED, RANDOM_SEED);

        // Sets generations. configuration.
        driver.setMaximumNumberOfGenerations(MAXIMUM_NUMBER_OF_GENERATIONS);

        // Executes the whole job.
        driver.run();
    }
}
