package it.unisa.elephant56.user.operators;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Defines the elitism function to select individuals among parents and offspring population.
 * <p>
 * It receives a list of individuals in input and must create a list of chosen individuals by the user criteria.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class SurvivalSelection<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

    /**
     * Constructs the instance of the class passing the properties.
     *
     * @param islandNumber         the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties       the properties defined by the user
     * @param configuration        the configuration
     */
    public SurvivalSelection(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

    /**
     * Selects the individuals among those of the current generation.
     * <p>
     * If not overridden, it returns the offspring population
     *
     * @param parentsPopulation   the list of initial individuals
     * @param offspringPopulation the list of individuals of the offspring
     * @return the list of chosen individuals
     */
    public List<IndividualWrapper<IndividualType, FitnessValueType>> selectSurvivors(
            List<IndividualWrapper<IndividualType, FitnessValueType>> parentsPopulation,
            List<IndividualWrapper<IndividualType, FitnessValueType>> offspringPopulation
    ) {
        return offspringPopulation;
    }
}
