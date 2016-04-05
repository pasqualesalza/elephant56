package it.unisa.elephant56.user.sample.operators.termination_condition_check;

import java.util.List;

import it.unisa.elephant56.user.operators.TerminationConditionCheck;
import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.sample.common.fitness_value.DoubleFitnessValue;
import it.unisa.elephant56.user.sample.common.fitness_value.NumberFitnessValue;

/**
 * Defines a termination criterion function that checks if the fitness value is permanently the same for a certain
 * amount of generations.
 *
 * To work, the fitness value must be a {@link NumberFitnessValue}.
 *
 * It needs some properties to be set in the properties object.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
@SuppressWarnings("rawtypes")
public class StableFitnessCheckTerminationConditionCheck<IndividualType extends Individual,
        FitnessValueType extends NumberFitnessValue>
		extends TerminationConditionCheck<IndividualType, FitnessValueType> {

	/**
	 * Defines the number of decimal digits to consider.
	 */	
	public final static String INT_NUMBER_OF_DECIMAL_DIGITS =
            "stable_fitness_check_termination.configuration.number_of_decimal_digits.int";
	
	/**
	 * Defines if to consider the upper limit as "strict" (default: false).
	 */	
	public final static String BOOLEAN_IS_UPPER_LIMIT_STRICT =
            "stable_fitness_check_termination.configuration.is_upper_limit_strict.boolean";
	
	/**
	 * Defines the upper limit.
	 */	
	public final static String LONG_UPPER_LIMIT =
            "stable_fitness_check_termination.configuration.upper_limit.long";
	
	/**
	 * Defines the stable generations counter.
	 */	
	public final static String LONG_STABLE_GENERATIONS_COUNTER =
            "stable_fitness_check_termination.configuration.stable_generations_counter.long";
	
	/**
	 * Defines the global previous max fitness value.
	 */	
	public final static String DOUBLE_GLOBAL_PREVIOUS_MAX_FITNESS_VALUE =
            "stable_fitness_check_termination.configuration.global_previous_max_fitness_value.double";

	/**
	 * Defines the current max fitness value.
	 */	
	public final static String DOUBLE_CURRENT_MAX_FITNESS_VALUE =
            "stable_fitness_check_termination.configuration.current_max_fitness_value.double";
	
	// Object fields.
	
	private Integer numberOfDecimalDigits;
	
	private boolean isUpperLimitStrict;
	private long upperLimit;
	
	/**
	 * Initialises the object and properties.
	 *
	 * @param userProperties the properties
	 * @param configuration the configuration
	 */
	public StableFitnessCheckTerminationConditionCheck(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
		
		userProperties.setLong(LONG_STABLE_GENERATIONS_COUNTER, 0L);
		userProperties.setDouble(DOUBLE_GLOBAL_PREVIOUS_MAX_FITNESS_VALUE, 0.0);
		userProperties.setDouble(DOUBLE_CURRENT_MAX_FITNESS_VALUE, 0.0);
		
		if (userProperties.isSet(INT_NUMBER_OF_DECIMAL_DIGITS)) {
			this.numberOfDecimalDigits = userProperties.getInt(INT_NUMBER_OF_DECIMAL_DIGITS, 2);
		} else {
			this.numberOfDecimalDigits = null;
		}
		
		this.isUpperLimitStrict = userProperties.getBoolean(BOOLEAN_IS_UPPER_LIMIT_STRICT, false);
		this.upperLimit = userProperties.getLong(LONG_UPPER_LIMIT, 0L);
	}
	
	@Override
	public boolean checkIndividualTerminationCondition(IndividualWrapper<IndividualType, FitnessValueType> individual) {
		return false;
	}

	@Override
	public boolean checkIslandTerminationCondition(
            List<IndividualWrapper<IndividualType, FitnessValueType>> individuals, Properties islandProperties
    ) {
		// Retrieves the current max fitness value.
		double currentMaxFitnessValue = 0.0;
		for (IndividualWrapper<IndividualType, FitnessValueType> individual : individuals) {
			double currentFitnessValue = individual.getFitnessValue().getNumber().doubleValue();
			int compare = 0;
			if (this.numberOfDecimalDigits != null) {
				compare = DoubleFitnessValue.compare(currentFitnessValue, currentMaxFitnessValue, this.numberOfDecimalDigits);
			} else {
				compare = Double.compare(currentFitnessValue, currentMaxFitnessValue);
			}
			if (compare > 0)
				currentMaxFitnessValue = currentFitnessValue;
		}
		
		// Updates the generations counter.
		long generationsCounter = this.getUserProperties().getLong(LONG_STABLE_GENERATIONS_COUNTER, 0L);
		this.getUserProperties().setLong(LONG_STABLE_GENERATIONS_COUNTER, (generationsCounter + 1L));
		
		// Writes the current max fitness value.
		this.getUserProperties().setDouble(DOUBLE_CURRENT_MAX_FITNESS_VALUE, currentMaxFitnessValue);
		
		// Returns false in any case.
		return false;
	}
	
	@Override
	public boolean checkGlobalTerminationCondition(List<Properties> properties, long generationNumber) {
		// Finds the previous and current global max fitness values.
		double globalPreviousMaxFitnessValue = properties.get(0).getDouble(DOUBLE_GLOBAL_PREVIOUS_MAX_FITNESS_VALUE, 0L);
		double globalCurrentMaxFitnessValue = 0.0;
		
		for (Properties currentIslandProperties : properties) {
			double currentMaxFitnessValue = currentIslandProperties.getDouble(DOUBLE_CURRENT_MAX_FITNESS_VALUE, 0L);
			int compare = 0;
			if (this.numberOfDecimalDigits != null) {
				compare = DoubleFitnessValue.compare(currentMaxFitnessValue, globalCurrentMaxFitnessValue, this.numberOfDecimalDigits);
			} else {
				compare = Double.compare(currentMaxFitnessValue, globalCurrentMaxFitnessValue);
			}
			if (compare > 0)
				globalCurrentMaxFitnessValue = currentMaxFitnessValue;
		}
		
		// Checks if the global fitness value is globally stable.
		boolean isGlobalMaxFitnessValueStable = false;
		if (this.numberOfDecimalDigits != null) {
			isGlobalMaxFitnessValueStable = DoubleFitnessValue.areEqual(globalPreviousMaxFitnessValue, globalCurrentMaxFitnessValue, this.numberOfDecimalDigits);
		} else {
			isGlobalMaxFitnessValueStable = (Double.compare(globalPreviousMaxFitnessValue, globalCurrentMaxFitnessValue) == 0);
		}
		
		// If true, increments the counter.
		if (isGlobalMaxFitnessValueStable) {
			// Checks the counter.
			long generationsCounter = properties.get(0).getLong(LONG_STABLE_GENERATIONS_COUNTER, 0L);
			boolean condition;
			if (this.isUpperLimitStrict) {
				condition = (generationsCounter > this.upperLimit);
			} else {
				condition = (generationsCounter >= this.upperLimit);
			}
			
			if (condition) {
				// Stops the execution.
				return true;
			}
			
			// It does not stop the execution, so it sets the properties.
			for (Properties currentIslandProperties : properties) {
				currentIslandProperties.setDouble(DOUBLE_GLOBAL_PREVIOUS_MAX_FITNESS_VALUE, globalCurrentMaxFitnessValue);
				currentIslandProperties.setDouble(DOUBLE_CURRENT_MAX_FITNESS_VALUE, 0.0);
			}
		} else {
			// Reset the properties fields.
			for (Properties currentIslandProperties : properties) {
				currentIslandProperties.setLong(LONG_STABLE_GENERATIONS_COUNTER, 0L);
				currentIslandProperties.setDouble(DOUBLE_GLOBAL_PREVIOUS_MAX_FITNESS_VALUE, globalCurrentMaxFitnessValue);
				currentIslandProperties.setDouble(DOUBLE_CURRENT_MAX_FITNESS_VALUE, 0.0);
			}
		}
		
		return false;
	}
}
