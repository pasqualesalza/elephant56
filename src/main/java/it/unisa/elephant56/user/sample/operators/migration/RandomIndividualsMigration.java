package it.unisa.elephant56.user.sample.operators.migration;

import java.util.*;

import it.unisa.elephant56.util.common.Pair;
import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.Migration;

/**
 * Defines a migration function that randomly assigns the individuals to move.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public abstract class RandomIndividualsMigration
        <IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends Migration<IndividualType, FitnessValueType> {

	/**
	 * Defines the number of individuals to migrate.
	 */
	public final static String INT_NUMBER_OF_MIGRANT_INDIVIDUALS =
            "random_individuals_migration.configuration.number_of_migrant_individuals.int";

    public final static String LONG_RANDOM_SEED =
            "random_individuals_migration.configuration.random_seed.long";
    public final static String BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED =
            "random_individuals_migration.add_island_number_to_random_seed.boolean";
    private static final long DEFAULT_RANDOM_SEED = 0;

	protected Integer numberOfMigrantIndividuals;
	
	private Random random;
	
	public RandomIndividualsMigration(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
		
		this.numberOfMigrantIndividuals = userProperties.getInt(INT_NUMBER_OF_MIGRANT_INDIVIDUALS, 0);

        // Creates the random object.
        long randomSeed = this.getUserProperties().getLong(LONG_RANDOM_SEED, DEFAULT_RANDOM_SEED);
        boolean addIslandNumberToRandomSeed = this.getUserProperties().getBoolean(BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED, false);
        long finalRandomSeed = (addIslandNumberToRandomSeed) ? (randomSeed + this.getIslandNumber()) : randomSeed;
        this.random = new Random(finalRandomSeed);
	}
	
	/**
	 * Returns the destination island, according to the criterion defined by the user.
	 *
	 * @param currentIndividual the individual to assign to
	 * @param currentIndividualNumber the current number of the individual
	 * @param numberOfIndividualsToMigrate the total number of individuals to migrate
	 * @param totalNumberOfIndividuals the number of individuals in the population
	 * 
	 * @return the destination island
	 */
	protected abstract int getNextDestination(
            IndividualWrapper<IndividualType, FitnessValueType> currentIndividual,
            int currentIndividualNumber, int numberOfIndividualsToMigrate, int totalNumberOfIndividuals
    );
	
	@Override
    public List<Pair<IndividualWrapper<IndividualType, FitnessValueType>, Integer>> assign(
            List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
		// Creates the list of assignments and the set to keep track of the already selected individuals.
		List<Pair<IndividualWrapper<IndividualType, FitnessValueType>, Integer>> assignments =
                new ArrayList<>(this.numberOfMigrantIndividuals);

        // Copies the population to select individual to migrate.
        List<IndividualWrapper<IndividualType, FitnessValueType>> populationCopy =
                new ArrayList<IndividualWrapper<IndividualType, FitnessValueType>>(population);

		// Selects and assigns the individuals.
		for (int i = 0; i < this.numberOfMigrantIndividuals; i++) {
            // Selects the next individual.
			IndividualWrapper<IndividualType, FitnessValueType> selectedIndividual = getNextIndividual(populationCopy);

            // Removes the individual from the list.
            populationCopy.remove(selectedIndividual);;
			
			// Gets the destination.
			Integer currentDestination = getNextDestination(selectedIndividual, i, this.numberOfMigrantIndividuals,
                    populationCopy.size());

			// Adds the assignment to the map.
			assignments.add(new Pair<>(selectedIndividual, currentDestination));
		}
		
		// Returns the assignments.
		return assignments;
	}
	
	/**
	 * Retrieves the next individual by random.
	 *
	 * @param population the population
	 * 
	 * @return the next individual
	 */
	protected IndividualWrapper<IndividualType, FitnessValueType> getNextIndividual(
            List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
		// Select the individual.
		int randomNumber = this.random.nextInt(population.size());
		return population.get(randomNumber);
	}
}
