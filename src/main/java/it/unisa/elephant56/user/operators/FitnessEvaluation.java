package it.unisa.elephant56.user.operators;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines the fitness function to evaluate an individual.
 *
 * It must return a value of type {@link FitnessValue}, defined by the user.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class FitnessEvaluation<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

	/**
	 * Constructs the instance of the class passing the properties.
	 *
     * @param islandNumber the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties the properties defined by the user
     * @param configuration the configuration
	 */
	public FitnessEvaluation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

	/**
	 * Evaluates the individual in input.
	 *
	 * If not overridden, it returns null.
	 *
	 * @param individualWrapper the individual to evaluate
	 *
	 * @return the computed value
	 */
	public FitnessValueType evaluate(IndividualWrapper<IndividualType, FitnessValueType> individualWrapper) {
		return null;
	}
}