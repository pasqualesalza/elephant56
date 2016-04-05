package it.unisa.elephant56.user.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines the crossover function to generate a child from two parents.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class Crossover<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {
	
	/**
	 * Constructs the instance of the class passing the properties.
	 *
     * @param islandNumber the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties the properties defined by the user
     * @param configuration the configuration
	 */
	public Crossover(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
	}

	/**
	 * Combines two individual to make children.
	 *
	 * If not overridden, it returns the individuals themselves.
	 *
	 * @param individualWrapper1 the first individual
	 * @param individualWrapper2 the second individual
	 * @param coupleNumber the number of the couple
	 * @param totalNumberOfCouples the total of couples selected
	 * @param parentsPopulationSize the parents population size
	 * 
	 * @return the children
	 */
	public List<IndividualWrapper<IndividualType, FitnessValueType>> cross(
            IndividualWrapper<IndividualType, FitnessValueType> individualWrapper1,
            IndividualWrapper<IndividualType,FitnessValueType> individualWrapper2,
            int coupleNumber, int totalNumberOfCouples, int parentsPopulationSize
    ) {
		List<IndividualWrapper<IndividualType, FitnessValueType>> children = new ArrayList<IndividualWrapper<IndividualType, FitnessValueType>>(2);
		
		children.add(individualWrapper1);
		children.add(individualWrapper2);
		
		return children;
	}
}
