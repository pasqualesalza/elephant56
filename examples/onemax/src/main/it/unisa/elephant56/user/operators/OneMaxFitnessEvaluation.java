package it.unisa.elephant56.user.operators;

import org.apache.hadoop.conf.Configuration;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.user.common.BitStringIndividual;
import it.unisa.elephant56.user.sample.common.fitness_value.IntegerFitnessValue;

public class OneMaxFitnessEvaluation extends FitnessEvaluation<BitStringIndividual, IntegerFitnessValue> {
    public OneMaxFitnessEvaluation(Integer islandNumber, Integer totalNumberOfIslands, Properties userProperties, Configuration configuration) {
        super(islandNumber, totalNumberOfIslands, userProperties, configuration);
    }

    @Override
    public IntegerFitnessValue evaluate(IndividualWrapper<BitStringIndividual, IntegerFitnessValue> individualWrapper) {
        BitStringIndividual individual = individualWrapper.getIndividual();
        int count = 0;
        for (int i = 0; i < individual.size(); i++)
            if (individual.get(i))
                count++;
        return new IntegerFitnessValue(count);
    }
}
