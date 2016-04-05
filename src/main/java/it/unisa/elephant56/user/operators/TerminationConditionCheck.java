package it.unisa.elephant56.user.operators;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines the termination condition in order to terminate the generations.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 * 
 * @author Pasquale Salza
 */
public class TerminationConditionCheck<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

	/**
	 * Constructs the instance of the class passing the user.
	 *
     * @param islandNumber the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties the properties defined by the user
     * @param configuration the configuration
	 */
	public TerminationConditionCheck(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

	/**
	 * Checks if an individual satisfies the termination condition for the problem.
	 *
	 * It acts just after the fitness phase.
	 *
	 * If not overridden, it returns "false".
	 *
	 * @param individual the individual to check
	 * 
	 * @return "true" if it satisfies the termination condition
	 */
	public boolean checkIndividualTerminationCondition(IndividualWrapper<IndividualType, FitnessValueType> individual) {
		return false;
	}
	
	/**
	 * Checks if all island individuals satisfy the termination condition for the problem.
	 *
	 * It acts just at the end of the generation phase.
	 *
	 * If not overridden, it returns "false".
	 *
	 * @param individuals the individual to check
     * @param islandProperties the island properties
	 * 
	 * @return "true" if they satisfy the termination condition
	 */
	public boolean checkIslandTerminationCondition(
            List<IndividualWrapper<IndividualType, FitnessValueType>> individuals,
            Properties islandProperties
    ) {
		return false;
	}
	
	/**
	 * It check the termination condition with the proprerties by all the islands for the same generation. A previous
     * property change could be stored in one of the island properties.
	 *
	 * If not overridden, it returns "false".
	 *
	 * @param islandsProperties the properties, one for each island
	 * @param generationNumber the current number of generation
	 * 
	 * @return "true" if the termination condition is satisfied
	 */
	public boolean checkGlobalTerminationCondition(List<Properties> islandsProperties, long generationNumber) {
		return false;
	}
}
