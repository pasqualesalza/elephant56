package it.unisa.elephant56.user.sample.common.individual;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Defines a sequence individual of {@link Double}.
 */
public class DoubleSequenceIndividual
        extends SequenceIndividual<Double, DoubleSequenceIndividual, DoubleSequenceIndividual> {

    private ArrayList<Double> arrayList;

    /**
     * Constructs an empty sequence.
     */
    public DoubleSequenceIndividual() {
        this.arrayList = new ArrayList<Double>();
    }

    /**
     * Constructs a sequence specifying the size, incrementable adding more elements.
     *
     * @param size the size of the array list
     */
    public DoubleSequenceIndividual(int size) {
        this.arrayList = new ArrayList<Double>(size);
    }

    /**
     * Constructs a sequence specifying copying the one in input.
     *
     * @param original the sequence to copy
     */
    private DoubleSequenceIndividual(DoubleSequenceIndividual original) {
        this.arrayList = new ArrayList<Double>(original.size());

        for (Double value : original.arrayList)
            this.arrayList.add(value.doubleValue());
    }

    @Override
    protected Class<DoubleSequenceIndividual> getSequenceIndividualClass() {
        return DoubleSequenceIndividual.class;
    }

    @Override
    public int size() {
        return this.arrayList.size();
    }

    @Override
    public Double get(int index) {
        return this.arrayList.get(index);
    }

    @Override
    public void set(int index, Double element) {
        if (index < this.size()) {
            this.arrayList.set(index, element);
        } else if (index == this.size()) {
            this.arrayList.add(index, element);
        }
    }

    @Override
    public Iterator<Double> iterator() {
        return this.arrayList.iterator();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new DoubleSequenceIndividual(this);
    }

    @Override
    public int hashCode() {
        return this.arrayList.hashCode();
    }
}
