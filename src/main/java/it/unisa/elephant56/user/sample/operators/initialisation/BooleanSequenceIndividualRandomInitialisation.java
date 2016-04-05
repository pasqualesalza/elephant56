package it.unisa.elephant56.user.sample.operators.initialisation;

import it.unisa.elephant56.user.sample.common.individual.BooleanSequentialIndividual;
import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines an initialiser of BooleanSequenceIndividual.
 *
 * @param <FitnessValueType>
 */
public class BooleanSequenceIndividualRandomInitialisation<FitnessValueType extends FitnessValue>
        extends SequenceIndividualRandomInitialisation<BooleanSequentialIndividual, FitnessValueType, Boolean> {

	private static final List<Boolean> possibleElements = new ArrayList<>();
	static {
		possibleElements.add(true);
		possibleElements.add(false);
	}
	
	/**
	 * Constructs an instance.
	 *
	 * @param userProperties the user properties
	 * @param configuration the configuration
	 */
	public BooleanSequenceIndividualRandomInitialisation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration, Integer populationSize
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration, populationSize);
	}

	@Override
	protected List<Boolean> getPossibleElements() {
		return possibleElements;
	}
	
	@Override
	protected Class<BooleanSequentialIndividual> getSequenceIndividualClass() {
		return BooleanSequentialIndividual.class;
	}
}
