package it.unisa.elephant56.user.common;

/**
 * <p>
 * Defines an individual.
 * </p>
 * 
 * <p>
 * If not overridden, it identifies an empty individual.
 * </p>
 * 
 * @author Pasquale Salza
 */
public abstract class Individual implements Cloneable {

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