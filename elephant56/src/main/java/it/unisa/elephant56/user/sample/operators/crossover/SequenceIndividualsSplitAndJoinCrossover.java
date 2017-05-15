package it.unisa.elephant56.user.sample.operators.crossover;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.operators.Crossover;
import it.unisa.elephant56.user.sample.common.individual.SequenceIndividual;

/**
 * Defines a crossover function that split in one point and joins a couple of individuals making two children.
 * <p>
 * It needs some properties to be set in the properties object.
 *
 * @param <IndividualType>
 * @param <FitnessValueType>
 */
public class SequenceIndividualsSplitAndJoinCrossover
        <IndividualType extends SequenceIndividual<?, IndividualType, IndividualType>,
                FitnessValueType extends FitnessValue> extends Crossover<IndividualType, FitnessValueType> {

    public final static String LONG_RANDOM_SEED =
            "sequence_individuals_split_and_join_crossover.configuration.random_seed.long";
    public final static String BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED =
            "sequence_individuals_split_and_join_crossover.add_island_number_to_random_seed.boolean";

    private static final long DEFAULT_RANDOM_SEED = 0;

    protected Random random;

    public SequenceIndividualsSplitAndJoinCrossover(
            Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration
    ) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);

        // Creates the random object.
        long randomSeed = this.getUserProperties().getLong(LONG_RANDOM_SEED, DEFAULT_RANDOM_SEED);
        boolean addIslandNumberToRandomSeed = this.getUserProperties().getBoolean(BOOLEAN_ADD_ISLAND_NUMBER_TO_RANDOM_SEED, false);
        long finalRandomSeed = (addIslandNumberToRandomSeed) ? (randomSeed + this.getIslandNumber()) : randomSeed;
        this.random = new Random(finalRandomSeed);
    }

    @Override
    public List<IndividualWrapper<IndividualType, FitnessValueType>> cross(
            IndividualWrapper<IndividualType, FitnessValueType> individualWrapper1,
            IndividualWrapper<IndividualType, FitnessValueType> individualWrapper2,
            int coupleNumber, int totalNumberOfCouples, int parentsPopulationSize
    ) {
        List<IndividualWrapper<IndividualType, FitnessValueType>> children = new ArrayList<IndividualWrapper<IndividualType, FitnessValueType>>(parentsPopulationSize);

        // Retrieves the two individuals.
        IndividualType individual1 = individualWrapper1.getIndividual();
        IndividualType individual2 = individualWrapper2.getIndividual();

        // Splits both the individual in the same point.
        int minSize = Math.min(individual1.size(), individual2.size());

        int randomNumber = this.random.nextInt(minSize + 1);

        IndividualType[] individual1Splits = individual1.split(randomNumber);
        IndividualType[] individual2Splits = individual2.split(randomNumber);

        // Joins the parts.
        IndividualType child1 = individual1Splits[0].join(individual2Splits[1]);
        IndividualType child2 = individual2Splits[0].join(individual1Splits[1]);

        // Creates the two children.
        IndividualWrapper<IndividualType, FitnessValueType> childWrapper1 = new IndividualWrapper<IndividualType, FitnessValueType>();
        childWrapper1.setIndividual(child1);

        IndividualWrapper<IndividualType, FitnessValueType> childWrapper2 = new IndividualWrapper<IndividualType, FitnessValueType>();
        childWrapper2.setIndividual(child2);

        // Adds the first child and checks if adding the second one as well.
        children.add(childWrapper1);

        // Checks if writing the second individual.
        if (coupleNumber != (totalNumberOfCouples - 1)) {
            children.add(childWrapper2);
        } else {
            // It is the last couple, it does not write the second individual if the number of parents in the population
            // is odd.

            if (parentsPopulationSize % 2 == 0)
                children.add(childWrapper2);
        }

        return children;
    }
}
