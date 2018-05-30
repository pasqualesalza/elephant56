package it.unisa.elephant56.user.operators;

import java.util.Random;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.BitStringIndividual;
import it.unisa.elephant56.user.sample.common.fitness_value.IntegerFitnessValue;
import org.apache.hadoop.conf.Configuration;

public class BitStringMutation extends Mutation<BitStringIndividual, IntegerFitnessValue> {
    public static final String DOUBLE_PROBABILITY = "onemax.configuration.mutation.probability.double";
    public static final String LONG_RANDOM_SEED = "onemax.configuration.mutation.random_seed.long";

    private double probability;
    private Random random;

    public BitStringMutation(Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
        this.probability = userProperties.getDouble(DOUBLE_PROBABILITY, 0.5);
        this.random = new Random(userProperties.getLong(LONG_RANDOM_SEED, 0));
    }

    @Override
    public IndividualWrapper<BitStringIndividual, IntegerFitnessValue> mutate(IndividualWrapper<BitStringIndividual, IntegerFitnessValue> individualWrapper) {
        BitStringIndividual individual = individualWrapper.getIndividual();
        for (int i = 0; i < individual.size(); i++)
            if (random.nextDouble() <= this.probability)
                individual.set(i, !individual.get(i));
        return individualWrapper;
    }
}
