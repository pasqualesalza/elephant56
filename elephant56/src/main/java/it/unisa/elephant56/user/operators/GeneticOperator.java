package it.unisa.elephant56.user.operators;

import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import org.apache.hadoop.conf.Configuration;

public class GeneticOperator<IndividualType extends Individual, FitnessValueType extends FitnessValue> {

    protected int islandNumber;
    protected int totalNumberOfIslands;

    protected Properties userProperties;
    protected Configuration configuration;

    /**
     * Constructs the instance of the class passing the properties.
     *
     * @param islandNumber         the island number
     * @param totalNumberOfIslands the total number of islands
     * @param userProperties       the properties defined by the user
     * @param configuration        the configuration
     */
    public GeneticOperator(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        this.islandNumber = islandNumber;
        this.totalNumberOfIslands = totalNumberOfIslands;
        this.userProperties = userProperties;
        this.configuration = configuration;
    }

    public int getIslandNumber() {
        return this.islandNumber;
    }

    public int getTotalNumberOfIslands() {
        return this.totalNumberOfIslands;
    }

    /**
     * Returns the properties.
     *
     * @return the user properties
     */
    public Properties getUserProperties() {
        return this.userProperties;
    }

    /**
     * Returns the configuration.
     *
     * @return the configuration
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }
}
