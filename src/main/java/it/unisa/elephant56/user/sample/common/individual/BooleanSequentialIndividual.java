package it.unisa.elephant56.user.sample.common.individual;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Defines a sequence individual of {@link Boolean}.
 */
public class BooleanSequentialIndividual
		extends SequenceIndividual<Boolean, BooleanSequentialIndividual, BooleanSequentialIndividual> {

	private ArrayList<Boolean> arrayList;

	/**
	 * Constructs an empty sequence.
	 */
	public BooleanSequentialIndividual() {
		this.arrayList = new ArrayList<Boolean>();
	}

	/**
	 * Constructs a sequence specifying the size, incrementable adding more elements.
	 *
	 * @param size the size of the array list
	 */
	public BooleanSequentialIndividual(int size) {
		this.arrayList = new ArrayList<Boolean>(size);
	}

	/**
	 * Constructs a sequence specifying copying the one in input.
	 *
	 * @param original the sequence to copy
	 */
	private BooleanSequentialIndividual(BooleanSequentialIndividual original) {
		this.arrayList = new ArrayList<Boolean>(original.size());

		for (Boolean value : original.arrayList)
			this.arrayList.add(value.booleanValue());
	}

	@Override
	protected Class<BooleanSequentialIndividual> getSequenceIndividualClass() {
		return BooleanSequentialIndividual.class;
	}

	@Override
	public int size() {
		return this.arrayList.size();
	}

	@Override
	public Boolean get(int index) {
		return this.arrayList.get(index);
	}

	@Override
	public void set(int index, Boolean element) {
		if (index < this.size()) {
			this.arrayList.set(index, element);
		} else if (index == this.size()) {
			this.arrayList.add(index, element);
		}
	}

	@Override
	public Iterator<Boolean> iterator() {
		return this.arrayList.iterator();
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return new BooleanSequentialIndividual(this);
	}

	@Override
	public int hashCode() {
		return this.arrayList.hashCode();
	}
}
