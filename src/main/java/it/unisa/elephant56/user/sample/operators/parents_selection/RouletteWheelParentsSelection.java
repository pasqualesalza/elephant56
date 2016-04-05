package it.unisa.elephant56.user.sample.operators.parents_selection;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.ParentsSelection;
import it.unisa.elephant56.user.sample.common.fitness_value.NumberFitnessValue;
import it.unisa.elephant56.util.common.Pair;

/**
 * Defines a Roulette Wheel Parents Selection that chooses the couples by random and giving to the individuals with the best
 * fitness values more chances to be chosen.
 *
 * To work, the fitness value must be a {@link NumberFitnessValue}.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
@SuppressWarnings("rawtypes")
public class RouletteWheelParentsSelection<IndividualType extends Individual, FitnessValueType extends NumberFitnessValue>
		extends ParentsSelection<IndividualType, FitnessValueType> {

    public final static String LONG_RANDOM_SEED =
            "roulette_wheel_parents_selection.configuration.random_seed.long";
    public final static String BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED =
            "roulette_wheel_parents_selection.add_island_number_to_random_seed.boolean";
    private static final long DEFAULT_RANDOM_SEED = 0;

	private Random random;
	
	/**
	 * Constructs the instance.
	 */
	public RouletteWheelParentsSelection(
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
	public List<Pair<IndividualWrapper<IndividualType, FitnessValueType>,
            IndividualWrapper<IndividualType, FitnessValueType>>> selectParents(
            List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
		// Computes the number of couples.
		int numberOfCouples = (int) Math.ceil(((double) population.size() / 2.0));
		
		// Sums all the fitness values.
		double totalFitnessValue = 0.0;
		for (IndividualWrapper<IndividualType, FitnessValueType> individual : population) {
			FitnessValueType fitnessValue = individual.getFitnessValue();
			totalFitnessValue += fitnessValue.getNumber().doubleValue();
		}

        // Copies the population and the fitness value.
        List<IndividualWrapper<IndividualType, FitnessValueType>> currentPopulation = new ArrayList<>(population);
        double currentTotalFitnessValue = totalFitnessValue;
		
		// Generates the couples.
		List<Pair<IndividualWrapper<IndividualType, FitnessValueType>, IndividualWrapper<IndividualType, FitnessValueType>>> couples =
                new ArrayList<Pair<IndividualWrapper<IndividualType, FitnessValueType>, IndividualWrapper<IndividualType, FitnessValueType>>>(numberOfCouples);
		for (int i = 0; i < numberOfCouples; i++) {
			IndividualWrapper<IndividualType, FitnessValueType> individual1 =
                    getNextIndividual(currentTotalFitnessValue, currentPopulation);

            // Removes the selected individual.
            currentPopulation.remove(individual1);
            currentTotalFitnessValue -= individual1.getFitnessValue().getNumber().doubleValue();

			IndividualWrapper<IndividualType, FitnessValueType> individual2 =
                    getNextIndividual(currentTotalFitnessValue, currentPopulation);

            // Restores the current population.
            currentPopulation = new ArrayList<>(population);
            currentTotalFitnessValue = totalFitnessValue;

			couples.add(new Pair<>(individual1, individual2));
		}
		
		return couples;
	}
	
	/**
	 * Retrieves the next individual by selecting it with the wheel.
	 *
	 * @param totalFitnessValue the sum of all fitness values
	 * @param population the population
	 * 
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
