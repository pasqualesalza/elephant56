package it.unisa.elephant56.user.sample.common.fitness_value;

import it.unisa.elephant56.user.common.FitnessValue;

/**
 * Defines a fitness value that manages the {@link java.lang.Number} as value.
 *
 * @param <NumberType> the class to manage
 */
public abstract class NumberFitnessValue<NumberType extends Number>
        extends FitnessValue {

    /**
     * Sets the number value.
     *
     * @param number the value to set
     */
    public abstract void setNumber(NumberType number);

    /**
     * Gets the number value.
     *
     * @return the value to set
     */
    public abstract NumberType getNumber();

    @Override
    public String toString() {
        return getNumber().toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;

        result = prime * result
                + ((getNumber() == null) ? 0 : getNumber().hashCode());

        return result;
    }
}
