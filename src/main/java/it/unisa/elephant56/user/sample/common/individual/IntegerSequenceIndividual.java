package it.unisa.elephant56.user.sample.common.individual;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Defines a sequence individual of {@link Integer}.
 */
public class IntegerSequenceIndividual
		extends SequenceIndividual<Integer, IntegerSequenceIndividual, IntegerSequenceIndividual> {

	private ArrayList<Integer> arrayList;
	
	/**
	 * Constructs an empty sequence.
	 */
	public IntegerSequenceIndividual() {
		this.arrayList = new ArrayList<Integer>();
	}
	
	/**
	 * Constructs a sequence specifying the size, incrementable adding more elements.
	 *
	 * @param size the size of the array list
	 */
	public IntegerSequenceIndividual(int size) {
		this.arrayList = new ArrayList<Integer>(size);
	}
	
	/**
	 * Constructs a sequence specifying copying the one in input.
	 *
	 * @param original the sequence to copy
	 */
	private IntegerSequenceIndividual(IntegerSequenceIndividual original) {
		this.arrayList = new ArrayList<Integer>(original.size());
		
		for (Integer value : original.arrayList)
			this.arrayList.add(value.intValue());
	}
	
	@Override
	protected Class<IntegerSequenceIndividual> getSequenceIndividualClass() {
		return IntegerSequenceIndividual.class;
	}
	
	@Override
	public int size() {
		return this.arrayList.size();
	}

	@Override
	public Integer get(int index) {
		return this.arrayList.get(index);
	}

	@Override
	public void set(int index, Integer element) {
		if (index < this.size()) {
			this.arrayList.set(index, element);
		} else if (index == this.size()) {
			this.arrayList.add(index, element);
		}
	}
	
	@Override
	public Iterator<Integer> iterator() {
		return this.arrayList.iterator();
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return new IntegerSequenceIndividual(this);
	}

	@Override
	public int hashCode() {
		return this.arrayList.hashCode();
	}
}
