package it.unisa.elephant56.user.sample.operators.migration;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines a migration function move the selected individuals to the next island, considering the island like in a ring
 * network.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 * @author Pasquale Salza
 */
public class RingRandomIndividualsMigration<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends RandomIndividualsMigration<IndividualType, FitnessValueType> {

    public RingRandomIndividualsMigration(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

    @Override
    protected int getNextDestination(
            IndividualWrapper<IndividualType, FitnessValueType> currentIndividual,
            int currentIndividualNumber, int numberOfIndividualsToMigrate, int totalNumberOfIndividuals
    ) {
        return (this.islandNumber + 1) % this.totalNumberOfIslands;
    }

}
