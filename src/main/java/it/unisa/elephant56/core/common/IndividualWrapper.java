package it.unisa.elephant56.core.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;

import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Defines a wrapper of individual (class {@link it.unisa.elephant56.user.common.Individual}) with the possibility to
 * add a fitness value (class {@link it.unisa.elephant56.user.common.FitnessValue}) and to mark if an individual
 * satisfies the termination condition.
 */
public class IndividualWrapper<IndividualType extends Individual, FitnessValueType extends FitnessValue>
        implements Comparable<IndividualWrapper<IndividualType, FitnessValueType>>, Cloneable {

    private IndividualType individual;
    private FitnessValueType fitnessValue;
    private Boolean terminationConditionSatisfied;

    /**
     * Constructs an empty wrapper.
     */
    public IndividualWrapper() {
        this.individual = null;
        this.fitnessValue = null;
        this.terminationConditionSatisfied = null;
    }

    /**
     * Composes the Avro schema of the individual wrapper.
     *
     * @param individualClass   the class of the individual
     * @param fitnessValueClass the class of the fitness value
     * @return the schema composed
     */
    public static final Schema getSchema(Class<?> individualClass, Class<?> fitnessValueClass) {
        // Makes the list of the fields.
        List<Field> fields = new ArrayList<Field>();

        // Retrieves the schemas of the classes with the reflect.
        Schema individualReflectSchema = ReflectData.AllowNull.get().getSchema(individualClass);
        Schema fitnessValueReflectSchema = ReflectData.AllowNull.get().getSchema(fitnessValueClass);

        // Composes the schemas with the possibility to set values to null.
        Schema individualSchema = Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL), individualReflectSchema));
        Schema fitnessValueSchema = Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL), fitnessValueReflectSchema));
        Schema terminationConditionSatisfiedSchema = Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN)));

        // Adds the fields to the main schema.
        fields.add(new Field("individual", individualSchema, null, null));
        fields.add(new Field("fitnessValue", fitnessValueSchema, null, null));
        fields.add(new Field("terminationConditionSatisfied", terminationConditionSatisfiedSchema, null, null));

        // Creates the main schema and returns it.
        Schema schema = Schema.createRecord(IndividualWrapper.class.getName(), null,
                IndividualWrapper.class.getPackage().getName(), false);
        schema.setFields(fields);

        return schema;
    }

    /**
     * Empties the individual field.
     */
    public final void emptyIndividual() {
        this.individual = null;
    }

    /**
     * Checks if the individual is set.
     *
     * @return "true" if it is set
     */
    public final boolean isIndividualSet() {
        return (this.individual != null);
    }

    /**
     * Returns the individual inside the wrapper.
     *
     * @return the individual
     */
    public final IndividualType getIndividual() {
        return this.individual;
    }

    /**
     * Sets the individual.
     *
     * @param individual the individual ("null" to empty)
     */
    public final void setIndividual(IndividualType individual) {
        this.individual = individual;
    }

    /**
     * Empties the fitness value field.
     */
    public final void emptyFitnessValue() {
        this.fitnessValue = null;
    }

    /**
     * Checks if the fitness value is set.
     *
     * @return "true" if it is set
     */
    public final boolean isFitnessValueSet() {
        return (this.fitnessValue != null);
    }

    /**
     * Returns the fitness value of the individual.
     */
    public final FitnessValueType getFitnessValue() {
        return this.fitnessValue;
    }

    /**
     * Sets the fitness value of the individual.
     *
     * @param fitnessValue the fitness value ("null" to empty)
     */
    public final void setFitnessValue(FitnessValueType fitnessValue) {
        this.fitnessValue = fitnessValue;
    }

    /**
     * Sets if the individual satisfies the termination condition or not.
     *
     * @param value "true" if it satisfies the termination condition  ("null" to empty)
     */
    public final void setTerminationConditionSatisfied(Boolean value) {
        this.terminationConditionSatisfied = value;
    }

    /**
     * Empties the termination condition satisfied field. This means that the check has never been checked.
     */
    public final void emptyTerminationConditionSatisfied() {
        this.terminationConditionSatisfied = null;
    }

    /**
     * Checks if the individual satisfies the termination condition or not. If it returns the "null" value, it means
     * that is has not ever been checked.
     *
     * @return "true" if it satisfies the termination condition ("null" if never checked)
     */
    public final Boolean isTerminationConditionSatisfied() {
        return this.terminationConditionSatisfied;
    }

    /**
     * Checks if the termination condition satisfied flag is set.
     *
     * @return "true" if it is set
     */
    public final Boolean isTerminationConditionSatisfiedSet() {
        return (this.terminationConditionSatisfied != null);
    }

    /**
     * Returns the string representation of the wrapper.
     */
    @Override
    public final String toString() {
        return "{individual: " + this.individual + ", fitnessValue: " + this.fitnessValue +
                ", terminationConditionSatisfied: " + this.terminationConditionSatisfied + "}";
    }

    /**
     * Two individuals are equal only if all the fields are equal.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final boolean equals(Object object) {
        if (!(object instanceof IndividualWrapper))
            return false;

        IndividualWrapper<IndividualType, FitnessValueType> other =
                (IndividualWrapper<IndividualType, FitnessValueType>) object;

        return this.individual.equals(other.individual);
    }

    @Override
    public final int hashCode() {
        final int prime = 31;

        int result = 1;

        result = prime * result + ((this.fitnessValue == null) ? 0 : this.fitnessValue.hashCode());
        result = prime * result + ((this.individual == null) ? 0 : this.individual.hashCode());
        result = prime * result + ((this.terminationConditionSatisfied == null) ? 0
                : this.terminationConditionSatisfied.hashCode());

        return result;
    }



    /**
     * Compares by the fitness value.
     *
     * @param other the other individual wrapper to compare
     * @return a negative integer, zero, or a positive integer as this object is less than,
     * equal to, or greater than the specified object
     */
    @Override
    public int compareTo(IndividualWrapper<IndividualType, FitnessValueType> other) {
        if (this.fitnessValue == null)
            return 0;

        return this.fitnessValue.compareTo(other.getFitnessValue());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object clone() throws CloneNotSupportedException {
        IndividualType individualClone = (IndividualType) this.individual.clone();
        FitnessValueType fitnessValueClone = (this.fitnessValue != null)
                ? (FitnessValueType) this.fitnessValue.clone() : null;

        IndividualWrapper<IndividualType, FitnessValueType> clone =
                new IndividualWrapper<IndividualType, FitnessValueType>();
        clone.setIndividual(individualClone);
        clone.setFitnessValue(fitnessValueClone);
        clone.setTerminationConditionSatisfied(this.terminationConditionSatisfied);

        return clone;
    }
}
