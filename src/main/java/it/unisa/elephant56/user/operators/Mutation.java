package it.unisa.elephant56.user.operators;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines the mutation function that mutates an individual.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class Mutation<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

	/**
	 * Constructs the instance of the class passing the properties.
	 *
     * @param islandNumber the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties the properties defined by the user
     * @param configuration the configuration
	 */
	public Mutation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

	/**
	 * Mute the individual in input.
	 *
	 * If not overridden, it returns the same individual.
	 *
	 * @param individualWrapper the individual to mute
	 *
	 * @return the individual mutated
	 */
	public IndividualWrapper<IndividualType, FitnessValueType> mutate(
            IndividualWrapper<IndividualType, FitnessValueType> individualWrapper
    ) {
		return individualWrapper;
	}
}
