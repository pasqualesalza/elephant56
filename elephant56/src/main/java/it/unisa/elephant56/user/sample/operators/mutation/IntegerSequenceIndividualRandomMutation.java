package it.unisa.elephant56.user.sample.operators.mutation;

import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.sample.common.individual.IntegerSequenceIndividual;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

public class IntegerSequenceIndividualRandomMutation<FitnessValueType extends FitnessValue> extends SequenceIndividualRandomMutation<IntegerSequenceIndividual, FitnessValueType, Integer> {

    public final static String INT_MIN_NUMBER_ELEMENT =
            "integer_sequence_individual_random_mutation.configuration.min_number_element.int";
    public final static String INT_MAX_NUMBER_ELEMENT =
            "integer_sequence_individual_random_mutation.configuration.max_number_element.int";

    private final List<Integer> possibleElements;

    /**
     * Constructs an instance.
     *
     * @param userProperties the user properties
     */
    public IntegerSequenceIndividualRandomMutation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);

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
}
