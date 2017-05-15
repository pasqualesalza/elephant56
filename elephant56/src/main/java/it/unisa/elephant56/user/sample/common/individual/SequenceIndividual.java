package it.unisa.elephant56.user.sample.common.individual;

import java.lang.reflect.Array;

import it.unisa.elephant56.user.common.Individual;

/**
 * Defines an individual that collects a sequence of elements.
 *
 * @param <ElementType> the type of the elements
 * @param <SplitType>   the type of the split parts
 * @param <JoinType>    the type of the resulting join individual
 */
public abstract class SequenceIndividual<ElementType, SplitType extends SequenceIndividual<ElementType, SplitType, JoinType>,
        JoinType extends SequenceIndividual<ElementType, SplitType, JoinType>>
        extends Individual
        implements Iterable<ElementType>, SplittableIndividual<SplitType, Integer>, JoinableIndividual<JoinType> {

    public SequenceIndividual() {
    }

    /**
     * Defines the initial size of the sequence individual.
     *
     * @param size the size of the sequence
     */
    public SequenceIndividual(int size) {
    }

    /**
     * Returns the class of the sequence individual.
     *
     * @return the class of the sequence individual
     */
    protected abstract Class<? extends SequenceIndividual<ElementType, SplitType, JoinType>>
    getSequenceIndividualClass();

    /**
     * Returns the number of elements in the sequence.
     *
     * @return the number of elements
     */
    public abstract int size();

    /**
     * Returns the element in the "index" position.
     *
     * @param index the index of the element
     * @return the element
     */
    public abstract ElementType get(int index);

    /**
     * Sets the element in the "index" position.
     *
     * @param index   the index of the element
     * @param element the element to set
     */
    public abstract void set(int index, ElementType element);

    @SuppressWarnings("unchecked")
    @Override
    public SplitType[] split(Integer... splitPoints) {
        // Computes the two parts size.
        int size1 = splitPoints[0];
        int size2 = this.size() - splitPoints[0];

        // Splits the individual in two parts.
        SplitType part1;
        SplitType part2;
        try {
            part1 = (SplitType) getSequenceIndividualClass().getConstructor(int.class).newInstance(size1);
            part2 = (SplitType) getSequenceIndividualClass().getConstructor(int.class).newInstance(size2);
        } catch (Exception exception) {
            return null;
        }

        // Fills the individuals.
        for (int i = 0; i < size1; i++)
            part1.set(i, this.get(i));
        for (int i = 0; i < size2; i++)
            part2.set(i, this.get(size1 + i));

        // Returns the parts.
        SplitType[] result = (SplitType[]) Array.newInstance(getSequenceIndividualClass(), 2);

        result[0] = part1;
        result[1] = part2;

        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JoinType join(JoinType other) {
        // Joins the two lists.
        JoinType result = null;
        try {
            result = (JoinType) getSequenceIndividualClass().getConstructor(int.class).newInstance(this.size() + other.size());
        } catch (Exception exception) {
            return null;
        }

        int index = 0;
        for (int j = 0; j < this.size(); j++, index++)
            result.set(index, this.get(j));
        for (int j = 0; j < other.size(); j++, index++)
            result.set(index, other.get(j));

        // Returns the result.
        return result;
    }

    @Override
    public String toString() {
        if (this.size() == 0)
            return "()";

        String result = "(";

        for (int i = 0; i < this.size(); i++)
            result += this.get(i).toString() + ", ";

        result = result.substring(0, result.length() - 2);

        result += ")";

        return result;
    }
}
