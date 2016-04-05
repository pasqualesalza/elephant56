package it.unisa.elephant56.user.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import it.unisa.elephant56.util.common.Pair;
import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines the migration function to migrate individuals.
 *
 * It receive a list of individuals in input and must create a list of the individual chosen by the user criteria to
 * migrate with the relative destination island.
 * 
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class Migration<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends GeneticOperator<IndividualType, FitnessValueType> {

	/**
	 * Constructs the instance of the class passing the properties.
	 *
     * @param islandNumber the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties the properties defined by the user
     * @param configuration the configuration
	 */
	public Migration(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

	/**
	 * Selects the individuals among those of the current generation. If the returning map is empty the default behavior
     * is not to migrate the missing individuals.
	 *
	 * If not overridden, it returns an empty map.
	 *
	 * @param population the list of individuals
	 *
	 * @return the map of assignments
	 */
	public List<Pair<IndividualWrapper<IndividualType, FitnessValueType>, Integer>> assign(
            List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
		return new ArrayList<>(0);
	}
}
