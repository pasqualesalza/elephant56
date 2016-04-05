package it.unisa.elephant56.user.sample.common.individual;

import it.unisa.elephant56.user.common.Individual;

/**
 * <p>
 * Defines an individual that is able to be split into some parts,
 * according to the split points.
 * </p>
 * 
 * @author Pasquale Salza
 *
 * @param <IndividualType> the type of the individuals
 * @param <SplitPointType> the type of the split points
 */
public interface SplittableIndividual<IndividualType extends Individual, SplitPointType> {

	public IndividualType[] split(SplitPointType... splitPoints);
	
}