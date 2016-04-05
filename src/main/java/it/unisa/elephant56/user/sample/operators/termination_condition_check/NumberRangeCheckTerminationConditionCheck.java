package it.unisa.elephant56.user.sample.operators.termination_condition_check;

import it.unisa.elephant56.user.operators.TerminationConditionCheck;
import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.sample.common.fitness_value.NumberFitnessValue;

/**
 * Defines a termination criterion function that checks if the fitness value of the individual in input is in the
 * numeric range:
 *
 * <code>LOWER_LIMIT <= FITNESS VALUE <= UPPER_LIMIT</code>
 * 
 * To work, the fitness value must be a {@link NumberFitnessValue}.
 *
 * It needs some properties to be set in the properties object.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
@SuppressWarnings("rawtypes")
public class NumberRangeCheckTerminationConditionCheck<IndividualType extends Individual,
        FitnessValueType extends NumberFitnessValue>
        extends TerminationConditionCheck<IndividualType, FitnessValueType> {

	/**
	 * Defines if to consider the lower limit as "strict".
	 */
	public final static String BOOLEAN_IS_LOWER_LIMIT_STRICT =
            "number_range_check_termination.configuration.is_lower_limit_strict.boolean";

    /**
	 * Defines if to consider the upper limit as "strict".
	 */	
	public final static String BOOLEAN_IS_UPPER_LIMIT_STRICT =
            "number_range_check_termination.configuration.is_upper_limit_strict.boolean";
	
	/**
	 * Defines the lower limit of the range.
	 */
	public final static String DOUBLE_LOWER_LIMIT =
            "number_range_check_termination.configuration.lower_limit.double";
	
	/**
	 * Defines the upper limit of the range.
	 */
	public final static String DOUBLE_UPPER_LIMIT =
            "number_range_check_termination.configuration.upper_limit.double";
	
	private boolean isLowerLimitStrict;
	private boolean isUpperLimitStrict;
	
	private Double lowerLimit;
	private Double upperLimit;

	public NumberRangeCheckTerminationConditionCheck(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
		
		this.isLowerLimitStrict = userProperties.getBoolean(BOOLEAN_IS_LOWER_LIMIT_STRICT, false);
		this.isUpperLimitStrict = userProperties.getBoolean(BOOLEAN_IS_UPPER_LIMIT_STRICT, false);
		
		this.lowerLimit = userProperties.getDouble(DOUBLE_LOWER_LIMIT, 0D);
		this.upperLimit = userProperties.getDouble(DOUBLE_UPPER_LIMIT, 0D);
	}
	
	@Override
	public boolean checkIndividualTerminationCondition(IndividualWrapper<IndividualType, FitnessValueType> individual) {
		// Retrieves the double value from the fitness value.
		Double fitnessValue = individual.getFitnessValue().getNumber().doubleValue();
		
		// Returns boolean only if the fitness value is in the range.
		int compare = Double.compare(fitnessValue, this.lowerLimit);
		
		boolean condition1 = (compare > 0);
		if (!this.isLowerLimitStrict)
			condition1 |= (compare == 0);

		compare = Double.compare(fitnessValue, this.upperLimit);
		
		boolean condition2 = (compare < 0);
		if (!this.isUpperLimitStrict)
			condition2 |= (compare == 0);

		return (condition1 && condition2);
	}
}
