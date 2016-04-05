package it.unisa.elephant56.user.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.util.common.Pair;

/**
 * Defines the selection function to select individuals among those of the current generation.
 *
 * It receive a list of individuals in input and must create a list of the couples of individual chosen by the user
 * criteria.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class ParentsSelection<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

	/**
	 * Constructs the instance of the class passing the properties.
	 *
     * @param islandNumber the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties the properties defined by the user
     * @param configuration the configuration
	 */
	public ParentsSelection(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

    /**
     * Selects the couples (pairs) of individuals among those of the current generation. The number of couples in output
     * has to be respected.
     *
     * If not overridden, it returns an empty list.
     *
     * @param population the list of individuals
     *
     * @return the list of couples
     */
    public List<Pair<IndividualWrapper<IndividualType, FitnessValueType>,
            IndividualWrapper<IndividualType, FitnessValueType>>>
    selectParents(List<IndividualWrapper<IndividualType, FitnessValueType>> population) {
        List<Pair<IndividualWrapper<IndividualType, FitnessValueType>,
                IndividualWrapper<IndividualType, FitnessValueType>>> result =new ArrayList<Pair<IndividualWrapper<IndividualType, FitnessValueType>, IndividualWrapper<IndividualType, FitnessValueType>>>(0);
        return result;
    }
}
