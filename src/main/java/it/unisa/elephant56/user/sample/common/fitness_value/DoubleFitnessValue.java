package it.unisa.elephant56.user.sample.common.fitness_value;

import it.unisa.elephant56.user.common.FitnessValue;

/**
 * Defines a fitness value of type double.
 */
public class DoubleFitnessValue
		extends NumberFitnessValue<Double> {

	private Double number;
	
	/**
	 * Constructs a double fitness value.
	 */
	public DoubleFitnessValue() {
		this.number = 0.0;
	}
	
	/**
	 * Constructs a double fitness value, specifying the value.
	 *
	 * @param number the value to set
	 */
	public DoubleFitnessValue(Double number) {
		this.number = number;
	}
	
	@Override
	public void setNumber(Double number) {
		this.number = number;
	}

	@Override
	public Double getNumber() {
		return this.number;
	}
	
	@Override
	public boolean equals(Object object) {
		DoubleFitnessValue other = (DoubleFitnessValue) object;
		
		return this.number.equals(other.number);
	}
	
	@Override
	public int compareTo(FitnessValue other) {
		if (other == null)
			return 1;
		
		Double otherInteger = ((DoubleFitnessValue) other).getNumber();
		
		return this.number.compareTo(otherInteger);
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		Double valueClone = new Double(this.number);
		
		DoubleFitnessValue clone = new DoubleFitnessValue();
		
		clone.setNumber(valueClone);
		
		return clone;
	}
	
	/**
	 * Compares two double values considering a certain number of decimal digits.
	 * 
	 * @param numberOfDecimalDigits the number of decimal digits
	 * @return "0" if equal, "-1" if value1 < value2, "1" otherwise
	 */
	public static int compare(double value1, double value2, int numberOfDecimalDigits) {
		double decimalMultiplier = Math.pow(10, numberOfDecimalDigits);
		
		double roundedValue1 = (double) Math.round(value1 * decimalMultiplier) / decimalMultiplier;
		double roundedValue2 = (double) Math.round(value2 * decimalMultiplier) / decimalMultiplier;
		
		return Double.compare(roundedValue1, roundedValue2);
	}
	
	/**
	 * Checks if two double values are equal considering a certain number of decimal digits.
	 * 
	 * @param numberOfDecimalDigits the number of decimal digits
	 * @return "true" if equal, "false" otherwise
	 */
	public static boolean areEqual(double value1, double value2, int numberOfDecimalDigits) {
		return (compare(value1, value2, numberOfDecimalDigits) == 0);
	}
	
	/**
	 * Compares two values considering a certain number of decimal digits.
	 * 
	 * @param other the other number to compare to
	 * @param numberOfDecimalDigits the number of decimal digits
	 * @return "0" if equal, "-1" if value1 < value2, "1" otherwise
	 */
	public int compareTo(DoubleFitnessValue other, int numberOfDecimalDigits) {
		return compare(this.getNumber(), other.getNumber(), numberOfDecimalDigits);
	}
	
	/**
	 * Checks if two values are equal considering a certain number of decimal digits.
	 * 
	 * @param other the other number to compare to
	 * @param numberOfDecimalDigits the number of decimal digits
	 * @return "true" if equal, "false" otherwise
	 */
	public boolean equals(DoubleFitnessValue other, int numberOfDecimalDigits) {
		return (this.compareTo(other, numberOfDecimalDigits) == 0);
	}
}
