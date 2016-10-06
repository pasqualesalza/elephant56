package it.unisa.elephant56.user.sample.common.individual;

import it.unisa.elephant56.user.common.Individual;

/**
 * Defines an individual that is able to be joined with another to generate a new one.
 *
 * @param <IndividualType> the type of the individual
 */
public interface JoinableIndividual<IndividualType extends Individual> {

    /**
     * Join the individual with the "other" individual and returns the generated individual.
     *
     * @param other the other individual to join with
     * @return the individual generated
     */
    public IndividualType join(IndividualType other);
}
