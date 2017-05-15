package it.unisa.elephant56.util.common;

/**
 * Defines a pair of elements.
 *
 * @param <FirstElementType>  the type of the first element
 * @param <SecondElementType> the type of the second element
 */
public class Pair<FirstElementType, SecondElementType> {

    private FirstElementType firstElement;
    private SecondElementType secondElement;

    /**
     * Constructs an empty pair.
     */
    public Pair() {
        this.firstElement = null;
        this.secondElement = null;
    }

    /**
     * Constructs a pair setting the elements too.
     *
     * @param firstElement  the first element
     * @param secondElement the second element
     */
    public Pair(FirstElementType firstElement, SecondElementType secondElement) {
        this.firstElement = firstElement;
        this.secondElement = secondElement;
    }

    /**
     * Sets the first element.
     *
     * @param firstElement the first element
     */
    public void setFirstElement(FirstElementType firstElement) {
        this.firstElement = firstElement;
    }

    /**
     * Sets the second element.
     *
     * @param secondElement the second element
     */
    public void setSecondElement(SecondElementType secondElement) {
        this.secondElement = secondElement;
    }

    /**
     * Returns the first element.
     *
     * @return the first element
     */
    public FirstElementType getFirstElement() {
        return this.firstElement;
    }

    /**
     * Returns the second element.
     *
     * @return the second element
     */
    public SecondElementType getSecondElement() {
        return this.secondElement;
    }

    @Override
    public String toString() {
        return "(" + this.firstElement + ", " + this.secondElement + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((firstElement == null) ? 0 : firstElement.hashCode());
        result = prime * result + ((secondElement == null) ? 0 : secondElement.hashCode());
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object object) {
        Pair<FirstElementType, SecondElementType> other = (Pair<FirstElementType, SecondElementType>) object;
        return this.firstElement.equals(other.firstElement)
                && this.secondElement.equals(other.secondElement);
    }
}
