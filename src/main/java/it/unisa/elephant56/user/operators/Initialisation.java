package it.unisa.elephant56.user.operators;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines the initialisation function to generate an individual.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 * 
 * @author Pasquale Salza
 */
public class Initialisation<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

    private int populationSize;

	/**
	 * Constructs the instance of the class passing the properties.
	 *
     * @param islandNumber the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties the properties defined by the user
     * @param configuration the configuration
     * @param populationSize the number of individuals to generate
	 */
	public Initialisation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration,
            Integer populationSize
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);

        this.populationSize = populationSize;
    }

    /**
     * Returns the number of individuals to generate.
     *
     * @return the number of individuals to generate
     */
    protected int getPopulationSize() {
        return this.populationSize;
    }

	/**
	 * Generates the next individual.
	 *
	 * If not overridden, it returns an empty individual.
	 *
	 * @param id the identify code of the individual to generate
	 *
	 * @return the individual generated
	 */
	public IndividualWrapper<IndividualType, FitnessValueType> generateNextIndividual(int id) {
		return new IndividualWrapper<>();
	}
}
