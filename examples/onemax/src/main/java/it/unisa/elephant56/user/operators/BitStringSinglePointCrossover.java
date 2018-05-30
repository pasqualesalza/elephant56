package it.unisa.elephant56.user.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.BitStringIndividual;
import it.unisa.elephant56.user.sample.common.fitness_value.IntegerFitnessValue;

public class BitStringSinglePointCrossover extends Crossover<BitStringIndividual, IntegerFitnessValue> {
    public static final String LONG_RANDOM_SEED = "onemax.configuration.crossover.random_seed.long";

    private Random random;

    public BitStringSinglePointCrossover(Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);

        this.random = new Random(userProperties.getLong(LONG_RANDOM_SEED, 0));
    }

    @Override
    public List<IndividualWrapper<BitStringIndividual, IntegerFitnessValue>> cross(IndividualWrapper<BitStringIndividual, IntegerFitnessValue> individualWrapper1, IndividualWrapper<BitStringIndividual, IntegerFitnessValue> individualWrapper2, int coupleNumber, int totalNumberOfCouples, int parentsPopulationSize) {
        BitStringIndividual parent1 = individualWrapper1.getIndividual();
        BitStringIndividual parent2 = individualWrapper2.getIndividual();

        int cutPoint = random.nextInt(parent1.size());

        BitStringIndividual child1 = new BitStringIndividual(parent1.size());
        BitStringIndividual child2 = new BitStringIndividual(parent2.size());

        for (int i = 0; i < cutPoint; i++) {
            child1.set(i, parent1.get(i));
            child2.set(i, parent2.get(i));
        }

        for (int i = cutPoint; i< parent1.size(); i++) {
            child1.set(i, parent2.get(i));
            child2.set(i, parent1.get(i));
        }

        List<IndividualWrapper<BitStringIndividual, IntegerFitnessValue>> children = new ArrayList<>(2);

        IndividualWrapper<BitStringIndividual, IntegerFitnessValue> childWrapper1 = new IndividualWrapper<>();
        childWrapper1.setIndividual(child1);

        IndividualWrapper<BitStringIndividual, IntegerFitnessValue> childWrapper2 = new IndividualWrapper<>();
        childWrapper2.setIndividual(child2);

        children.add(childWrapper1);
        children.add(childWrapper2);

        return children;
    }
}
