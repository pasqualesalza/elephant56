package it.unisa.elephant56.user.sample.operators.survival_selection;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.SurvivalSelection;
import it.unisa.elephant56.user.sample.common.fitness_value.NumberFitnessValue;
import org.apache.hadoop.conf.Configuration;

import java.util.*;

/**
 * Defines a Roulette Wheel Survival Selection that chooses the individuals by random and giving to the individuals with the best
 * fitness values more chances to be chosen.
 * <p>
 * To work, the fitness value must be a {@link it.unisa.elephant56.user.sample.common.fitness_value.NumberFitnessValue}.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
@SuppressWarnings("rawtypes")
public class RouletteWheelSurvivalSelection<IndividualType extends Individual, FitnessValueType extends NumberFitnessValue>
        extends SurvivalSelection<IndividualType, FitnessValueType> {

    public final static String LONG_RANDOM_SEED =
            "roulette_wheel_survival_selection.configuration.random_seed.long";
    public final static String BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED =
            "roulette_wheel_survival_selection.add_island_number_to_random_seed.boolean";
    private static final long DEFAULT_RANDOM_SEED = 0;

    private Random random;

    /**
     * Constructs the instance.
     */
    public RouletteWheelSurvivalSelection(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);

        // Creates the random object.
        long randomSeed = this.getUserProperties().getLong(LONG_RANDOM_SEED, DEFAULT_RANDOM_SEED);
        boolean addIslandNumberToRandomSeed = this.getUserProperties().getBoolean(BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED, false);
        long finalRandomSeed = (addIslandNumberToRandomSeed) ? (randomSeed + this.getIslandNumber()) : randomSeed;
        this.random = new Random(finalRandomSeed);
    }

    @Override
    public List<IndividualWrapper<IndividualType, FitnessValueType>> selectSurvivors(
            List<IndividualWrapper<IndividualType, FitnessValueType>> parentsPopulation,
            List<IndividualWrapper<IndividualType, FitnessValueType>> offspringPopulation
    ) {
        // Computes the number of survivors.
        int numberOfSurvivors = parentsPopulation.size();

        // Builds a list with both groups.
        List<IndividualWrapper<IndividualType, FitnessValueType>> population = new ArrayList<IndividualWrapper<IndividualType, FitnessValueType>>(parentsPopulation);
        population.addAll(offspringPopulation);

        // Sums all the fitness values.
        double totalFitnessValue = 0.0;
        for (IndividualWrapper<IndividualType, FitnessValueType> individual : population) {
            FitnessValueType fitnessValue = individual.getFitnessValue();
            totalFitnessValue += fitnessValue.getNumber().doubleValue();
        }

        // Selects the survivors.
        List<IndividualWrapper<IndividualType, FitnessValueType>> survivors =
                new ArrayList<>(numberOfSurvivors);

        for (int i = 0; i < numberOfSurvivors; i++) {
            IndividualWrapper<IndividualType, FitnessValueType> selectedIndividual = getNextIndividual(totalFitnessValue, population);

            // Removes the selected individual.
            population.remove(selectedIndividual);
            totalFitnessValue -= selectedIndividual.getFitnessValue().getNumber().doubleValue();

            survivors.add(selectedIndividual);
        }

        return survivors;
    }

    /**
     * Retrieves the next individual by selecting it with the wheel.
     *
     * @param totalFitnessValue the sum of all fitness values
     * @param population        the population
     * @return the next individual
     */
    protected IndividualWrapper<IndividualType, FitnessValueType> getNextIndividual(
            double totalFitnessValue, List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
        // Flows the roulette until reached the right individual.
        double randomNumber = this.random.nextDouble() * totalFitnessValue;

        double currentSum = 0.0;
        int index = 0;
        int lastAddedIndex = 0;

        while ((currentSum < randomNumber) && (index < population.size())) {
            IndividualWrapper<IndividualType, FitnessValueType> individual = population.get(index);
            FitnessValueType fitnessValue = individual.getFitnessValue();
            currentSum += fitnessValue.getNumber().doubleValue();
            lastAddedIndex = index;
            index++;
        }

        return population.get(lastAddedIndex);
    }
}
