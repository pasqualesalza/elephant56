package it.unisa.elephant56.user.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines the elitism function to select individuals among those of the current generation.
 * <p>
 * It receives a list of individuals in input and must create a list of chosen individuals by the user criteria.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class Elitism<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

    /**
     * Constructs the instance of the class passing the properties.
     *
     * @param islandNumber         the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties       the properties defined by the user
     * @param configuration        the configuration
     */
    public Elitism(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

    /**
     * Selects the individuals among those of the current generation.
     * <p>
     * If not overridden, it returns an empty elite.
     *
     * @param population the list of individuals
     * @return the list of chosen individuals
     */
    public List<IndividualWrapper<IndividualType, FitnessValueType>> selectElite(
            List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
        return new ArrayList<IndividualWrapper<IndividualType, FitnessValueType>>(0);
    }
}
