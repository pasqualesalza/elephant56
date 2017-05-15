package it.unisa.elephant56.user.sample.operators.elitism;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.Elitism;

/**
 * Defines an elitism function that chooses the best individuals among the population in input, by the fitness value.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class BestIndividualsElitism<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        extends Elitism<IndividualType, FitnessValueType> {

    public final static String INT_NUMBER_OF_ELITISTS =
            "best_individuals_elitism.configuration.number_of_elitists.int";

    private int numberOfElitists;

    /**
     * Constructs the instance.
     */
    public BestIndividualsElitism(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);

        this.numberOfElitists = userProperties.getInt(INT_NUMBER_OF_ELITISTS, 0);
    }

    @Override
    public List<IndividualWrapper<IndividualType, FitnessValueType>> selectElite(
            List<IndividualWrapper<IndividualType, FitnessValueType>> population
    ) {
        // Sorts the population by the fitness value.
        Collections.sort(population, Collections.reverseOrder());

        // Selects the individuals.
        List<IndividualWrapper<IndividualType, FitnessValueType>> elitePopulation =
                new ArrayList<IndividualWrapper<IndividualType, FitnessValueType>>(numberOfElitists);
        for (int i = 0; i < numberOfElitists; i++)
            elitePopulation.add(population.get(i));

        // Returns the selection.
        return elitePopulation;
    }
}
