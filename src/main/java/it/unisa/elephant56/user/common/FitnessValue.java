package it.unisa.elephant56.user.common;

/**
 * <p>
 * Defines the type of the return value of a fitness evaluation.
 * </p>
 * <p>
 * <p>
 * It must define the way to compare with other objects of the same class.
 * </p>
 *
 * @author Pasquale Salza
 */
public abstract class FitnessValue
        implements Comparable<FitnessValue> {

    public FitnessValue() {
    }

    /**
     * <p>
     * Compares the current fitness value with another. Pay attention to control
     * if the value is null.
     * </p>
     */
    @Override
    public abstract int compareTo(FitnessValue other);

    /**
     * <p>
     * Clones the fitness value.
     * </p>
     */
    @Override
    public abstract Object clone() throws CloneNotSupportedException;

    /**
     * <p>
     * Retrieves the unique hash code of the object.
     * </p>
     */
    @Override
    public abstract int hashCode();

}