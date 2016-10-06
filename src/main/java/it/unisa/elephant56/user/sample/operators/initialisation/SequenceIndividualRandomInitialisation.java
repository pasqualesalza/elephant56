package it.unisa.elephant56.user.sample.operators.initialisation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.operators.Initialisation;
import it.unisa.elephant56.user.sample.common.individual.SequenceIndividual;

/**
 * Defines an initialisation function that generates sequence individual specifying the set of possible elements.
 * <p>
 * It needs some properties to be set in the properties object.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
@SuppressWarnings("rawtypes")
public abstract class SequenceIndividualRandomInitialisation<IndividualType extends SequenceIndividual,
        FitnessValueType extends FitnessValue, ElementType>
        extends Initialisation<IndividualType, FitnessValueType> {

    /**
     * Defines if the size is a fixed or a maximum value.
     */
    public final static String BOOLEAN_IS_NUMBER_OF_ELEMENTS_RANDOM =
            "random_sequence_individual_initialisation.configuration.is_number_of_elements_random.boolean";

    public final static String INT_NUMBER_OF_ELEMENTS =
            "random_sequence_individual_initialisation.configuration.number_of_elements.int";

    public final static String BOOLEAN_REPEAT_ELEMENTS =
            "random_sequence_individual_initialisation.configuration.repeat_elements.boolean";

    public final static String LONG_RANDOM_SEED =
            "random_sequence_individual_initialisation.configuration.random_seed.long";
    public final static String BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED =
            "random_sequence_individual_initialisation.add_island_number_to_random_seed.boolean";
    private static final long DEFAULT_RANDOM_SEED = 0;

    private boolean isRandomNumberOfElements;
    private Integer numberOfElements;
    private boolean repeatElements;
    protected Random random;

    public SequenceIndividualRandomInitialisation(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration, Integer populationSize
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration, populationSize);

        this.isRandomNumberOfElements = userProperties.getBoolean(BOOLEAN_IS_NUMBER_OF_ELEMENTS_RANDOM, false);
        this.numberOfElements = userProperties.getInt(INT_NUMBER_OF_ELEMENTS, 0);

        this.repeatElements = userProperties.getBoolean(BOOLEAN_REPEAT_ELEMENTS, true);

        // Creates the random object.
        long randomSeed = this.getUserProperties().getLong(LONG_RANDOM_SEED, DEFAULT_RANDOM_SEED);
        boolean addIslandNumberToRandomSeed = this.getUserProperties().getBoolean(BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED, false);
        long finalRandomSeed = (addIslandNumberToRandomSeed) ? (randomSeed + this.getIslandNumber()) : randomSeed;
        this.random = new Random(finalRandomSeed);
    }

    /**
     * Returns the possible elements that an individual can contain.
     *
     * @return the list of possible elements
     */
    protected abstract List<ElementType> getPossibleElements();

    /**
     * Returns the class of the sequence individual.
     *
     * @return the class of the sequence individual
     */
    protected abstract Class<IndividualType> getSequenceIndividualClass();

    @SuppressWarnings("unchecked")
    @Override
    public IndividualWrapper<IndividualType, FitnessValueType> generateNextIndividual(int id) {
        // Creates the wrapper.
        IndividualWrapper<IndividualType, FitnessValueType> individualWrapper =
                new IndividualWrapper<IndividualType, FitnessValueType>();

        List<ElementType> possibleElements = new ArrayList<>(getPossibleElements());

        // Defines the size.
        int size = (this.isRandomNumberOfElements)
                ? this.random.nextInt(this.numberOfElements + 1) : this.numberOfElements;

        // Generates the elements.
        IndividualType individual;
        try {
            individual = getSequenceIndividualClass().getConstructor(int.class).newInstance(size);
        } catch (Exception exception) {
            return null;
        }
        for (int i = 0; i < size; i++) {
            int randomNumber = this.random.nextInt(possibleElements.size());
            ElementType element = possibleElements.get(randomNumber);
            individual.set(i, element);
            if (!this.repeatElements)
                possibleElements.remove(randomNumber);
        }

        // Sets the individual into the wrapper.
        individualWrapper.setIndividual(individual);

        // Returns the wrapper.
        return individualWrapper;
    }
}
