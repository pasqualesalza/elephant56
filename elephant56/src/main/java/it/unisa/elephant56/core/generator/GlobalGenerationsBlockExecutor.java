package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.GlobalDistributedDriver;
import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GlobalGenerationsBlockExecutor extends GenerationsBlockExecutor {

    private GlobalDistributedDriver globalDistributedDriver;

    public GlobalGenerationsBlockExecutor(GlobalDistributedDriver globalDistributedDriver) {
        super();

        this.globalDistributedDriver = globalDistributedDriver;
    }

    private void evaluateFitness(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance,
            GlobalDistributedDriver.FitnessEvaluationType fitnessEvaluationType
    ) throws IOException {
        try {
            Boolean[] selectedMarks = new Boolean[population.size()];
            List<IndividualWrapper<Individual, FitnessValue>> populationToDistribute = new ArrayList<>(population.size());

            // Finds the individuals with fitness value missing.
            for (int currentIndividualIndex = 0; currentIndividualIndex < population.size(); currentIndividualIndex++) {
                IndividualWrapper<Individual, FitnessValue> currentIndividual = population.get(currentIndividualIndex);

                if (!currentIndividual.isFitnessValueSet()) {
                    selectedMarks[currentIndividualIndex] = true;
                    populationToDistribute.add(currentIndividual);
                } else {
                    selectedMarks[currentIndividualIndex] = false;
                }
            }

            // Executes the distributed fitness evaluation.
            if (populationToDistribute.size() > 0) {
                globalDistributedDriver.runDistributedFitnessEvaluation(populationToDistribute, fitnessEvaluationType);

                // Retrieves the output population.
                List<IndividualWrapper<Individual, FitnessValue>> outputPopulation = globalDistributedDriver.getResultPopulation(fitnessEvaluationType);

                // Fills the population by the marks list.
                Iterator<IndividualWrapper<Individual, FitnessValue>> outputPopulationIterator = outputPopulation.iterator();
                for (int currentIndividualIndex = 0; currentIndividualIndex < population.size(); currentIndividualIndex++) {
                    if (selectedMarks[currentIndividualIndex] == true) {
                        population.get(currentIndividualIndex).setFitnessValue(outputPopulationIterator.next().getFitnessValue());
                    }
                }
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @Override
    protected void runFitnessEvaluation(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance
    ) throws IOException {
        evaluateFitness(population, fitnessEvaluationClassInstance, GlobalDistributedDriver.FitnessEvaluationType.PARENTS);
    }

    @Override
    protected void runOffspringFitnessEvaluation(
            List<IndividualWrapper<Individual, FitnessValue>> offspringPopulation,
            FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance
    ) throws IOException {
        evaluateFitness(offspringPopulation, fitnessEvaluationClassInstance, GlobalDistributedDriver.FitnessEvaluationType.OFFSPRING);
    }
}
