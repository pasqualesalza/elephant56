package it.unisa.elephant56.user.sample.operators.initialisation;

import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.sample.common.individual.IntegerSequenceIndividual;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines an initialiser of BitSequenceIndividual.
 *
 * @param <FitnessValueType>
 */
public class IntegerSequenceIndividualRandomInitialisation<FitnessValueType extends FitnessValue>
        extends SequenceIndividualRandomInitialisation<IntegerSequenceIndividual, FitnessValueType, Integer> {

    public final static String INT_MIN_NUMBER_ELEMENT =
            "integer_sequence_individual_random_initialisation.configuration.min_number_element.int";
    public final static String INT_MAX_NUMBER_ELEMENT =
            "integer_sequence_individual_random_initialisation.configuration.max_number_element.int";

    private final List<Integer> possibleElements;

    /**
     * Constructs an instance.
     *
     * @param userProperties the user properties
     * @param configuration  the configuration
     */
    public IntegerSequenceIndividualRandomInitialisation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration, Integer populationSize
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration, populationSize);

        Integer minNumberElement = this.getUserProperties().getInt(INT_MIN_NUMBER_ELEMENT, 0);
        Integer maxNumberElement = this.getUserProperties().getInt(INT_MAX_NUMBER_ELEMENT, Integer.MAX_VALUE);

        this.possibleElements = new ArrayList<>(maxNumberElement - minNumberElement + 1);
        for (int currentNumberElement = minNumberElement; currentNumberElement <= maxNumberElement; currentNumberElement++)
            this.possibleElements.add(currentNumberElement);
    }

    @Override
    protected List<Integer> getPossibleElements() {
        return this.possibleElements;
    }

    @Override
    protected Class<IntegerSequenceIndividual> getSequenceIndividualClass() {
        return IntegerSequenceIndividual.class;
    }
}
