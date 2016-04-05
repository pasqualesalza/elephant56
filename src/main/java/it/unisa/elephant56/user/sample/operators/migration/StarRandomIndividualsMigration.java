package it.unisa.elephant56.user.sample.operators.migration;

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
 * Defines a migration function move the selected individuals to all the islands, one per each. It work only if the
 * number of the individuals to migrate is a multiple of the number of islands - 1.
 * 
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class StarRandomIndividualsMigration<IndividualType extends Individual, FitnessValueType extends FitnessValue>
		extends RandomIndividualsMigration<IndividualType, FitnessValueType> {

	private int counter;
	
	public StarRandomIndividualsMigration(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
	}
	
	@Override
	public List<Pair<IndividualWrapper<IndividualType, FitnessValueType>, Integer>> assign(
            List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
		// Checks if the number of individuals to migrate is a multiple of the number of islands - 1.
		if ((this.numberOfMigrantIndividuals % (totalNumberOfIslands - 1)) != 0)
			return new ArrayList<>(0);
		
		return super.assign(population);
	}
	
	@Override
	protected int getNextDestination(
            IndividualWrapper<IndividualType, FitnessValueType> currentIndividual,
            int currentIndividualNumber, int numberOfIndividualsToMigrate, int numberOfIndividuals) {
		int result = (counter + 1) % (this.totalNumberOfIslands - 1);
		counter++;
		return result;
	}
}
