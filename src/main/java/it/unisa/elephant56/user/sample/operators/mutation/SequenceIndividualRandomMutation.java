package it.unisa.elephant56.user.sample.operators.mutation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.operators.Mutation;
import it.unisa.elephant56.user.sample.common.individual.SequenceIndividual;

/**
 * Defines a mutation function that generates sequence individuals specifying the set of possible elements.
 * <p>
 * It needs some properties to be set in the properties object.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
@SuppressWarnings("rawtypes")
public abstract class SequenceIndividualRandomMutation<IndividualType extends SequenceIndividual,
        FitnessValueType extends FitnessValue, ElementType>
        extends Mutation<IndividualType, FitnessValueType> {

    /**
     * Defines the mutation probability of an element.
     */
    public final static String DOUBLE_ELEMENT_MUTATION_PROBABILITY =
            "sequence_individual_random_mutation.configuration.element_mutation_probability.double";

    public final static String BOOLEAN_REPEAT_ELEMENTS =
            "sequence_individual_random_mutation.configuration.repeat_elements.boolean";

    public final static String LONG_RANDOM_SEED =
            "sequence_individual_random_mutation.configuration.random_seed.long";
    public final static String BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED =
            "sequence_individual_random_mutation.add_island_number_to_random_seed.boolean";
    private static final long DEFAULT_RANDOM_SEED = 0;

    private Double probabilityToMuteAnElement;

    private boolean repeatElements;

    private Random random;

    public SequenceIndividualRandomMutation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);

        this.probabilityToMuteAnElement = userProperties.getDouble(DOUBLE_ELEMENT_MUTATION_PROBABILITY, 0.0);

        this.repeatElements = userProperties.getBoolean(BOOLEAN_REPEAT_ELEMENTS, true);

        // Creates the random object.
        long randomSeed = this.getUserProperties().getLong(LONG_RANDOM_SEED, DEFAULT_RANDOM_SEED);
        boolean addIslandNumberToRandomSeed = this.getUserProperties().getBoolean(BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED, false);
        long finalRandomSeed = (addIslandNumberToRandomSeed) ? (randomSeed + this.getIslandNumber()) : randomSeed;
        this.random = new Random(finalRandomSeed);
    }

    /**
     * Returns the possible elements that an element of the individual can assume.
     *
     * @return the array of possible elements
     */
    protected abstract List<ElementType> getPossibleElements();

    @SuppressWarnings("unchecked")
    @Override
    public IndividualWrapper<IndividualType, FitnessValueType> mutate(
            IndividualWrapper<IndividualType, FitnessValueType> individualWrapper
    ) {
        // Retrieves the list of possible elements.
        List<ElementType> possibleElements = new ArrayList<>(getPossibleElements());

        // Retrieves the individual.
        IndividualType individual = individualWrapper.getIndividual();

        if (individual == null)
            return individualWrapper;

        // If it is not possible to repeat elements, remove the current from the set.
        if (!this.repeatElements) {
            for (int i = 0; i < individual.size(); i++) {
                ElementType currentElement = (ElementType) individual.get(i);
                possibleElements.remove(currentElement);
            }
        }

        // Possibly mutate the elements.
        for (int i = 0; i < individual.size(); i++) {
            // Checks if mutate or not.
            if (this.random.nextDouble() <= this.probabilityToMuteAnElement) {
                ElementType currentElement = (ElementType) individual.get(i);

                // Choose the element to substitute the current with.
                int randomNumber = this.random.nextInt(possibleElements.size());
                ElementType element = possibleElements.get(randomNumber);

                // Removes the element from the possible elements list.
                if (!this.repeatElements) {
                    possibleElements.remove(element);
                    possibleElements.add(currentElement);
                }

                // Substitutes the element.
                individual.set(i, element);
            }
        }

        // Returns the wrapper.
        return individualWrapper;
    }
}
