package it.unisa.elephant56.user.sample.common.fitness_value;

import it.unisa.elephant56.user.common.FitnessValue;

/**
 * Defines a fitness value of type integer.
 */
public class IntegerFitnessValue
		extends NumberFitnessValue<Integer> {
	
	private Integer number;
	
	/**
	 * Constructs a integer fitness value.
	 */
	public IntegerFitnessValue() {
		this.number = 0;
	}
	
	/**
	 * Constructs a integer fitness value, specifying the value.
	 *
	 * @param number the value to set
	 */
	public IntegerFitnessValue(Integer number) {
		this.number = number;
	}
	
	@Override
	public void setNumber(Integer number) {
		this.number = number;
	}

	@Override
	public Integer getNumber() {
		return this.number;
	}
	
	@Override
	public boolean equals(Object object) {
		IntegerFitnessValue other = (IntegerFitnessValue) object;
		
		return this.number.equals(other.number);
	}
	
	@Override
	public int compareTo(FitnessValue other) {
		if (other == null)
			return 1;
		
		Integer otherInteger = ((IntegerFitnessValue) other).getNumber();
		
		return this.number.compareTo(otherInteger);
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		Integer valueClone = new Integer(this.number);
		
		IntegerFitnessValue clone = new IntegerFitnessValue();
		
		clone.setNumber(valueClone);
		
		return clone;
	}
}
