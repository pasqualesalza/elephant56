package it.unisa.elephant56.user.sample.operators.mutation;

import it.unisa.elephant56.user.sample.common.individual.BooleanSequentialIndividual;
import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;

import java.util.ArrayList;
import java.util.List;

public class BooleanSequenceIndividualRandomMutation<FitnessValueType extends FitnessValue> extends SequenceIndividualRandomMutation<BooleanSequentialIndividual, FitnessValueType, Boolean> {

	private static final List<Boolean> possibleElements = new ArrayList<>();
	static {
		possibleElements.add(true);
		possibleElements.add(false);
	}
	
	/**
	 * Constructs an instance.
	 *
	 * @param userProperties the user properties
	 */
	public BooleanSequenceIndividualRandomMutation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
	}

	@Override
	protected List<Boolean> getPossibleElements() {
		return possibleElements;
	}
}
