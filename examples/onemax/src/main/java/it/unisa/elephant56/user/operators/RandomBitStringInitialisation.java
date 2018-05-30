package it.unisa.elephant56.user.operators;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.BitStringIndividual;
import it.unisa.elephant56.user.sample.common.fitness_value.IntegerFitnessValue;

public class RandomBitStringInitialisation extends Initialisation<BitStringIndividual, IntegerFitnessValue> {
    public static final String INT_INDIVIDUAL_SIZE = "onemax.configuration.initialisation.individual_size.int";
    public static final String LONG_RANDOM_SEED = "onemax.configuration.initialisation.random_seed.long";

    private int individualSize;
    private Random random;

    public RandomBitStringInitialisation(Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration, Integer populationSize) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration, populationSize);
        this.individualSize = userProperties.getInt(INT_INDIVIDUAL_SIZE, 0);
        this.random = new Random(userProperties.getLong(LONG_RANDOM_SEED, 0));
    }

    @Override
    public IndividualWrapper<BitStringIndividual, IntegerFitnessValue> generateNextIndividual(int id) {
        BitStringIndividual individual = new BitStringIndividual(this.individualSize);
        for (int i = 0; i < this.individualSize; i++)
            individual.set(i, random.nextBoolean());
        IndividualWrapper<BitStringIndividual, IntegerFitnessValue> individualWrapper = new IndividualWrapper<>();
        individualWrapper.setIndividual(individual);
        return individualWrapper;
    }
}
