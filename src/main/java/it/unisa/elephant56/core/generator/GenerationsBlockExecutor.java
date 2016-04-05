package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.core.reporter.individual.IndividualReporter;
import it.unisa.elephant56.core.reporter.time.GeneticOperatorsTimeReporter;
import it.unisa.elephant56.core.reporter.time.GenerationsBlockTimeReporter;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.user.operators.*;
import it.unisa.elephant56.user.operators.Initialisation;
import it.unisa.elephant56.user.operators.TerminationConditionCheck;
import it.unisa.elephant56.util.common.Pair;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class GenerationsBlockExecutor {
    protected long generationsBlockNumber;

    protected int initialisationPopulationSize;

    protected long startGenerationNumber;
    protected long finishGenerationNumber;

    protected long currentGenerationNumber;
    protected long lastExecutedGenerationNumber;

    protected Class<? extends Initialisation> initiliasationClass;
    protected Class<? extends FitnessEvaluation> fitnessEvaluationClass;
    protected Class<? extends TerminationConditionCheck> terminationConditionCheckClass;
    protected Class<? extends Elitism> elitismClass;
    protected Class<? extends ParentsSelection> parentsSelectionClass;
    protected Class<? extends Crossover> crossoverClass;
    protected Class<? extends Mutation> mutationClass;
    protected Class<? extends SurvivalSelection> survivalSelectionClass;
    protected Class<? extends Migration> migrationClass;

    protected boolean isInitialisationActive;
    protected boolean isSurvivalSelectionActive;
    protected boolean isElitismActive;
    protected boolean isMigrationActive;

    protected int nodeNumber;
    protected int totalNumberOfNodes;

    protected Properties userProperties;
    protected Configuration configuration;

    protected boolean isTimeReporterActive;
    protected GeneticOperatorsTimeReporter geneticOperatorsTimeReporter;
    protected GenerationsBlockTimeReporter generationsBlockTimeReporter;

    protected boolean isIndividualReporterActive;
    protected IndividualReporter individualReporter;

    protected List<IndividualWrapper<Individual, FitnessValue>> inputPopulation;
    protected List<IndividualWrapper<Individual, FitnessValue>> outputPopulation;

    protected List<IndividualWrapper<Individual, FitnessValue>> solutionsPopulation;
    protected List<IndividualWrapper<Individual, FitnessValue>> nonsolutionsPopulation;

    protected List<Pair<IndividualWrapper<Individual, FitnessValue>, Integer>> migrationAssignments;

    public GenerationsBlockExecutor() {
        this.isTimeReporterActive = false;
        this.isIndividualReporterActive = false;
        this.isInitialisationActive = false;
        this.isElitismActive = false;
        this.isSurvivalSelectionActive = false;
        this.isMigrationActive = false;
    }

    public long getGenerationsBlockNumber() {
        return generationsBlockNumber;
    }

    public void setGenerationsBlockNumber(long generationsBlockNumber) {
        this.generationsBlockNumber = generationsBlockNumber;
    }

    public void setInitialisationPopulationSize(int numberOfIndividuals) {
        this.initialisationPopulationSize = numberOfIndividuals;
    }

    public void setStartGenerationNumber(long generationNumber) {
        this.startGenerationNumber = generationNumber;
    }

    public void setFinishGenerationNumber(long generationNumber) {
        this.finishGenerationNumber = generationNumber;
    }

    public void setInitiliasationClass(
            Class<? extends Initialisation> initiliasationClass) {
        this.initiliasationClass = initiliasationClass;
    }

    public void setFitnessEvaluationClass(
            Class<? extends FitnessEvaluation> fitnessEvaluationClass) {
        this.fitnessEvaluationClass = fitnessEvaluationClass;
    }

    public void setTerminationConditionCheckClass(
            Class<? extends TerminationConditionCheck> terminationConditionCheckClass) {
        this.terminationConditionCheckClass = terminationConditionCheckClass;
    }

    public void setElitismClass(Class<? extends Elitism> elitismClass) {
        this.elitismClass = elitismClass;
    }

    public void setParentsSelectionClass(Class<? extends ParentsSelection> parentsSelectionClass) {
        this.parentsSelectionClass = parentsSelectionClass;
    }

    public void setCrossoverClass(Class<? extends Crossover> crossoverClass) {
        this.crossoverClass = crossoverClass;
    }

    public void setMutationClass(Class<? extends Mutation> mutationClass) {
        this.mutationClass = mutationClass;
    }

    public void setSurvivalSelectionClass(Class<? extends SurvivalSelection> survivalSelectionClass) {
        this.survivalSelectionClass = survivalSelectionClass;
    }

    public void setMigrationClass(Class<? extends Migration> migrationClass) {
        this.migrationClass = migrationClass;
    }

    public void activateInitialisation(boolean active) {
        this.isInitialisationActive = active;
    }

    public void activateElitism(boolean active) {
        this.isElitismActive = active;
    }

    public void activateSurvivalSelection(boolean active) {
        this.isSurvivalSelectionActive = active;
    }

    public void activateMigration(boolean active) {
        this.isMigrationActive = active;
    }

    public boolean isInitialisationActive() {
        return this.isInitialisationActive;
    }

    public boolean isSurvivalSelectionActive() {
        return this.isSurvivalSelectionActive;
    }

    public boolean isElitismActive() {
        return this.isElitismActive;
    }

    public boolean isMigrationActive() {
        return this.isMigrationActive;
    }

    public void setGeneticOperatorsTimeReporter(GeneticOperatorsTimeReporter reporter) {
        this.geneticOperatorsTimeReporter = reporter;
    }

    public void setGenerationsBlockTimeReporter(GenerationsBlockTimeReporter reporter) {
        this.generationsBlockTimeReporter = reporter;
    }

    public void setIndividualReporter(IndividualReporter reporter) {
        this.individualReporter = reporter;
    }

    public void activateTimeReporter(boolean active) {
        this.isTimeReporterActive = active;
    }

    public boolean isTimeReporterActive() {
        return this.isTimeReporterActive;
    }

    public void activateIndividualReporter(boolean active) {
        this.isIndividualReporterActive = active;
    }

    public boolean isIndividualReporterActive() {
        return this.isIndividualReporterActive;
    }

    public void setInputPopulation(List<IndividualWrapper<Individual, FitnessValue>> population) {
        this.inputPopulation = population;
        this.outputPopulation = population;
    }

    public List<IndividualWrapper<Individual, FitnessValue>> getOutputPopulation() {
        return this.outputPopulation;
    }

    public List<Pair<IndividualWrapper<Individual, FitnessValue>, Integer>> getMigrationAssignments() {
        return this.migrationAssignments;
    }

    public List<IndividualWrapper<Individual, FitnessValue>> getSolutionsPopulation() {
        return this.solutionsPopulation;
    }

    public List<IndividualWrapper<Individual, FitnessValue>> getNonsolutionsPopulation() {
        return this.nonsolutionsPopulation;
    }

    public long getLastExecutedGenerationNumber() {
        return this.lastExecutedGenerationNumber;
    }

    public long getCurrentGenerationNumber() {
        return this.currentGenerationNumber;
    }

    public Properties getUserProperties() {
        return this.userProperties;
    }

    public void setUserProperties(Properties userProperties) {
        this.userProperties = userProperties;
    }

    public int getNodeNumber() {
        return nodeNumber;
    }

    public void setNodeNumber(int nodeNumber) {
        this.nodeNumber = nodeNumber;
    }

    public int getTotalNumberOfNodes() {
        return totalNumberOfNodes;
    }

    public void setTotalNumberOfNodes(int totalNumberOfNodes) {
        this.totalNumberOfNodes = totalNumberOfNodes;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    @SuppressWarnings("unchecked")
    public void run()
            throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        // Registers the generations block start time.
        long generationsBlockStartTime = System.currentTimeMillis();

        // Instantiates the classes.
        Initialisation<Individual, FitnessValue> initalisationClassInstance = null;

        FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance = null;
        TerminationConditionCheck<Individual, FitnessValue> terminationConditionCheckClassInstance = null;
        Elitism<Individual, FitnessValue> elitismClassInstance = null;
        ParentsSelection<Individual, FitnessValue> parentsSelectionClassInstance = null;
        Crossover<Individual, FitnessValue> crossoverClassInstance= null;
        Mutation<Individual, FitnessValue> mutationClassInstance = null;
        SurvivalSelection<Individual, FitnessValue> survivalSelectionClassInstance = null;

        Migration<Individual, FitnessValue> migrationClassInstance = null;

        try {
            if (this.isInitialisationActive())
                initalisationClassInstance =
                        this.initiliasationClass
                                .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class,
                                        Integer.class)
                                .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                        this.configuration, this.initialisationPopulationSize);

            fitnessEvaluationClassInstance =
                    this.fitnessEvaluationClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            terminationConditionCheckClassInstance =
                    this.terminationConditionCheckClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            parentsSelectionClassInstance =
                    this.parentsSelectionClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            crossoverClassInstance =
                    this.crossoverClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);
            mutationClassInstance =
                    this.mutationClass
                            .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                            .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                    this.configuration);

            if (this.isSurvivalSelectionActive())
                survivalSelectionClassInstance =
                        this.survivalSelectionClass
                                .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                                .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                        this.configuration);

            if (this.isElitismActive())
                elitismClassInstance =
                        this.elitismClass
                                .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                                .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                        this.configuration);

            if (this.isMigrationActive())
                migrationClassInstance =
                        this.migrationClass
                                .getConstructor(Integer.class, Integer.class, Properties.class, Configuration.class)
                                .newInstance(this.nodeNumber, this.totalNumberOfNodes, this.userProperties,
                                        this.configuration);
        } catch (NullPointerException exception) {
            exception.printStackTrace();
            System.exit(-1);
        }

        // Initialises the input population if needed.
        if (this.isInitialisationActive()) {
            long initialisationStartTime = System.currentTimeMillis();
            this.inputPopulation = runInitialisation(initalisationClassInstance);
            long initialisationFinishTime = System.currentTimeMillis();

            if (this.isTimeReporterActive())
                this.generationsBlockTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                        this.currentGenerationNumber, GenerationsBlockTimeReporter.PhaseType.INITIALISATION,
                        initialisationStartTime, initialisationFinishTime);
        }

        // Instantiates the island properties.
        Properties islandProperties = new Properties();

        // Reads the input population and starts the generations.
        List<IndividualWrapper<Individual, FitnessValue>> currentPopulation = this.inputPopulation;

        this.currentGenerationNumber = this.startGenerationNumber;

        while (true) {
            // Registers the generation start time.
            long generationStartTime = System.currentTimeMillis();

            // Fitness evaluation.
            this.runFitnessEvaluation(currentPopulation, fitnessEvaluationClassInstance);

            // Individuals termination condition check.
            boolean isIndividualsTerminationConditionSatisfiedOnce =
                    this.runIndividualsTerminationConditionCheck(currentPopulation,
                            terminationConditionCheckClassInstance);

            // Checks if there are termination condition satisfaction notifications.
            boolean areThereTerminationConditionSatisfactionNotification =
                    this.checkTerminationConditionSatisfactionNotifications();
            if (areThereTerminationConditionSatisfactionNotification) {
                // Writes the time report.
                if (this.isTimeReporterActive())
                    this.generationsBlockTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                            this.currentGenerationNumber, GenerationsBlockTimeReporter.PhaseType.GENERATION,
                            generationStartTime, System.currentTimeMillis());

                // Stops the cycle.
                break;
            }
            // Checks if the island satisfies the termination condition.
            boolean isIslandTerminationConditionSatisfied = this.runIslandTerminationConditionCheck(currentPopulation,
                    islandProperties, terminationConditionCheckClassInstance);

            // Share the properties with other islands.
            // TODO Riabilitare accesso HDFS.
            //boolean isLastIslandInCurrentGeneration =
            //        this.shareIslandProperties(islandProperties, this.currentGenerationNumber);
            boolean isLastIslandInCurrentGeneration = false;

            // Checks if the termination condition is satisfied globally, only if it is the last island for the
            // generation.
            boolean isGlobalTerminationConditionSatisfied = false;
            if (isLastIslandInCurrentGeneration) {
                List<Properties> islandsProperties =
                        this.readSharedIslandProperties(islandProperties, this.currentGenerationNumber);
                isGlobalTerminationConditionSatisfied = this.runGlobalTerminationConditionCheck(islandsProperties,
                        this.currentGenerationNumber, terminationConditionCheckClassInstance);
            }

            // Checks if terminating for termination condition satisfaction.
            if (isIndividualsTerminationConditionSatisfiedOnce || isIslandTerminationConditionSatisfied ||
                    isGlobalTerminationConditionSatisfied) {
                // Notifies the termination condition satisfaction.
                // TODO Riabilitare accesso HDFS.
                //this.notifyTerminationConditionSatisfaction();

                // Writes the time report.
                if (this.isTimeReporterActive())
                    this.generationsBlockTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                            this.currentGenerationNumber, GenerationsBlockTimeReporter.PhaseType.GENERATION,
                            generationStartTime, System.currentTimeMillis());

                // Stops the cycle.
                break;
            }

            // Checks if terminating for finish generation number (+1) reached.
            if (this.currentGenerationNumber > this.finishGenerationNumber) {
                // Checks if migrating.
                if (this.isMigrationActive()) {
                    // Assign migrants to destinations.
                    this.migrationAssignments = runMigration(currentPopulation, migrationClassInstance);
                }
                
                // Writes the time report.
                if (this.isTimeReporterActive())
                    this.generationsBlockTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                        this.currentGenerationNumber, GenerationsBlockTimeReporter.PhaseType.GENERATION,
                        generationStartTime, System.currentTimeMillis());

                // Stops the cycle.
                break;
            }

            // Checks if doing elitism.
            List<IndividualWrapper<Individual, FitnessValue>> elitePopulation = null;
            if (this.isElitismActive()) {
                // Elitism.
                elitePopulation =
                        this.runElitism(currentPopulation, elitismClassInstance);

                // Subtracts the elite from the current population.
                for (IndividualWrapper<Individual, FitnessValue> elitist : elitePopulation)
                    currentPopulation.remove(elitist);
            }

            // Parents selection.
            List<Pair<IndividualWrapper<Individual, FitnessValue>,
                    IndividualWrapper<Individual, FitnessValue>>> selectedCouples =
                    this.runParentsSelection(currentPopulation, parentsSelectionClassInstance);

            // Crossover.
            List<IndividualWrapper<Individual, FitnessValue>> offspringPopulation =
                    this.runCrossover(selectedCouples, crossoverClassInstance);

            // Mutation.
            this.runMutation(offspringPopulation, mutationClassInstance);

            // Checks if doing survival selection.
            if (this.isSurvivalSelectionActive()) {
                // Evaluates fitness values for the offspring.
                this.runOffspringFitnessEvaluation(offspringPopulation, fitnessEvaluationClassInstance);

                // Survival selection.
                currentPopulation =
                        this.runSurvivalSelection(currentPopulation, offspringPopulation,
                                survivalSelectionClassInstance, fitnessEvaluationClassInstance);
            } else {
                currentPopulation = offspringPopulation;
            }

            // Joins the elite population.
            if (this.isElitismActive()) {
                currentPopulation.addAll(elitePopulation);
            }

            // Writes the time report.
            if (this.isTimeReporterActive())
                this.generationsBlockTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GenerationsBlockTimeReporter.PhaseType.GENERATION,
                    generationStartTime, System.currentTimeMillis());

            // Increments the generation number.
            this.currentGenerationNumber++;
        }

        // Registers the generations block finish time.
        long generationsBlockFinishTime = System.currentTimeMillis();

        // Writes the generation block time.
        if (this.isTimeReporterActive()) {
            this.generationsBlockTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber, -1L,
                    GenerationsBlockTimeReporter.PhaseType.GENERATIONS_BLOCK, generationsBlockStartTime,
                    generationsBlockFinishTime);
        }

        // Sets the output population.
        this.outputPopulation = currentPopulation;

        // Sets the last executed generation number.
        this.lastExecutedGenerationNumber = this.currentGenerationNumber - 1L;
    }

    protected List<IndividualWrapper<Individual, FitnessValue>> runInitialisation(
            Initialisation<Individual, FitnessValue> initialisationClassInstance
    ) throws IOException {
        // Iterates through the parents population.
        long currentStartTime;
        long currentFinishTime;
        long totalInitialisationTime = 0L;
        long numberOfInitialisations = 0L;

        List<IndividualWrapper<Individual, FitnessValue>> initialisationPopulation =
                new ArrayList<IndividualWrapper<Individual, FitnessValue>>(this.initialisationPopulationSize);

        for (int i = 0; i < initialisationPopulationSize; i++) {
            // Registers the current initialisation start time.
            currentStartTime = System.currentTimeMillis();

            // Calls the operator.
            IndividualWrapper<Individual, FitnessValue> individualWrapper =
                    initialisationClassInstance.generateNextIndividual(i);

            // Registers the current initialisation finish time.
            currentFinishTime = System.currentTimeMillis();

            // Stores the individual.
            initialisationPopulation.add(individualWrapper);

            // Updates the times.
            totalInitialisationTime += currentFinishTime - currentStartTime;
            numberOfInitialisations++;

        }

        // Writes the Initialisation times.
        if (this.isTimeReporterActive()) {
            long averageInitialisationTime = (numberOfInitialisations != 0L)
                    ? (long) Math.ceil((double) totalInitialisationTime / (double) numberOfInitialisations) : 0L;
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.AVERAGE_INITIALISATION, 0L,
                    averageInitialisationTime);
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.TOTAL_INITIALISATION, 0L,
                    totalInitialisationTime);
        }

        // Returns the result.
        return initialisationPopulation;
    }

    protected void runFitnessEvaluation(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance
    ) throws IOException {
        // Iterates through the parents population.
        long currentStartTime;
        long currentFinishTime;
        long currentFitnessEvaluationTime = 0L;
        long minFitnessEvaluationTime = Long.MAX_VALUE;
        long maxFitnessEvaluationTime = Long.MIN_VALUE;
        long totalFitnessEvaluationTime = 0L;
        long numberOfFitnessEvaluations = 0L;

        for (IndividualWrapper<Individual, FitnessValue> currentIndividual : population) {
            // Computes the fitness value for the current individual.
            if (!currentIndividual.isFitnessValueSet()) {
                // Registers the current fitness evaluation start time.
                currentStartTime = System.currentTimeMillis();

                // Calls the operator.
                FitnessValue currentFitnessValue = fitnessEvaluationClassInstance.evaluate(currentIndividual);

                // Registers the current fitness evaluation finish time.
                currentFinishTime = System.currentTimeMillis();

                // Updates the times.
                currentFitnessEvaluationTime = currentFinishTime - currentStartTime;
                if (currentFitnessEvaluationTime < minFitnessEvaluationTime)
                    minFitnessEvaluationTime = currentFitnessEvaluationTime;
                if (currentFitnessEvaluationTime > maxFitnessEvaluationTime)
                    maxFitnessEvaluationTime = currentFitnessEvaluationTime;
                totalFitnessEvaluationTime += currentFitnessEvaluationTime;
                numberOfFitnessEvaluations++;

                // Sets the found value inside the wrapper.
                currentIndividual.setFitnessValue(currentFitnessValue);
            }
        }

        // Writes the FitnessEvaluation times.
        if (this.isTimeReporterActive()) {
            if (numberOfFitnessEvaluations == 0L) {
                minFitnessEvaluationTime = 0;
                maxFitnessEvaluationTime = 0;
            }
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.MIN_FITNESS_EVALUATION, 0L,
                    minFitnessEvaluationTime);
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.MAX_FITNESS_EVALUATION, 0L,
                    maxFitnessEvaluationTime);
            long averageFitnessEvaluationTime = (numberOfFitnessEvaluations != 0L)
                    ? (long) Math.ceil((double) totalFitnessEvaluationTime / (double) numberOfFitnessEvaluations) : 0L;
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.AVERAGE_FITNESS_EVALUATION, 0L,
                    averageFitnessEvaluationTime);
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.TOTAL_FITNESS_EVALUATION, 0L,
                    totalFitnessEvaluationTime);
        }
    }

    protected boolean runIndividualsTerminationConditionCheck(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            TerminationConditionCheck<Individual, FitnessValue> terminationConditionCheckClassInstance
    ) throws IOException {
        // Iterates through the parents population.
        long currentStartTime;
        long currentFinishTime;
        long totalIndividualTerminationConditionChecksTime = 0L;
        long numberOfIndividualTerminationConditionChecks = 0L;

        boolean isTerminationConditionSatisfiedOnce = false;

        for (IndividualWrapper<Individual, FitnessValue> currentIndividual : population) {
            // Checks if the individual satisfied the termination condition.
            if (!currentIndividual.isTerminationConditionSatisfiedSet()) {
                // Registers the current termination condition start time.
                currentStartTime = System.currentTimeMillis();

                // Calls the operator.
                boolean currentIndividualSatisfiesTerminationCondition =
                        terminationConditionCheckClassInstance.checkIndividualTerminationCondition(currentIndividual);

                // Registers the current termination condition check finish time.
                currentFinishTime = System.currentTimeMillis();

                // Updates the times.
                totalIndividualTerminationConditionChecksTime += currentFinishTime - currentStartTime;
                numberOfIndividualTerminationConditionChecks++;

                // Stores the result in the individual.
                currentIndividual.setTerminationConditionSatisfied(currentIndividualSatisfiesTerminationCondition);

                // Updates the global flag.
                isTerminationConditionSatisfiedOnce |= currentIndividualSatisfiesTerminationCondition;
            }

            // Writes the individuals in the report.
            if (this.isIndividualReporterActive())
                this.individualReporter.writeIndividual(this.nodeNumber, this.generationsBlockNumber,
                        this.currentGenerationNumber, currentIndividual.getIndividual(),
                        currentIndividual.getFitnessValue(), currentIndividual.isTerminationConditionSatisfied());
        }

        // Writes the individuals termination condition checks times.
        if (this.isTimeReporterActive()) {
            long averageTerminationConditionCheckTime = (numberOfIndividualTerminationConditionChecks != 0L)
                    ? (long) Math.ceil((double) totalIndividualTerminationConditionChecksTime /
                    (double) numberOfIndividualTerminationConditionChecks) : 0L;
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.AVERAGE_INDIVIDUAL_TERMINATION_CONDITION_CHECK, 0L,
                    averageTerminationConditionCheckTime);
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_INDIVIDUAL_TERMINATION_CONDITION_CHECK, 0L,
                    totalIndividualTerminationConditionChecksTime);
        }

        // Returns the result.
        return isTerminationConditionSatisfiedOnce;
    }

    protected boolean runIslandTerminationConditionCheck(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            Properties islandProperties,
            TerminationConditionCheck<Individual, FitnessValue> terminationConditionCheckClassInstance
    ) throws IOException {
        // Registers the current island termination condition check start time.
        long startTime = System.currentTimeMillis();

        // Calls the function which checks the termination condition for the whole island.
        boolean isIslandTerminationConditionSatisfied =
                terminationConditionCheckClassInstance.checkIslandTerminationCondition(population, islandProperties);

        // Registers the current island termination condition finish time.
        long finishTime = System.currentTimeMillis();

        // Writes the island termination condition checks times.
        if (this.isTimeReporterActive())
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_ISLAND_TERMINATION_CONDITION_CHECK, startTime,
                    finishTime);

        // Returns the result.
        return isIslandTerminationConditionSatisfied;
    }

    protected boolean runGlobalTerminationConditionCheck(
            List<Properties> islandsProperties,
            long generationNumber,
            TerminationConditionCheck<Individual, FitnessValue> terminationConditionCheckClassInstance
    ) throws IOException {
        // Registers the gloabal termination condition check start time.
        long startTime = System.currentTimeMillis();

        // Calls the function which checks the termination condition for the global population.
        boolean isGlobalTerminationConditionSatisfied =
                terminationConditionCheckClassInstance.checkGlobalTerminationCondition(islandsProperties,
                        generationNumber);

        // Registers the global termination condition finish time.
        long finishTime = System.currentTimeMillis();

        // Writes the island termination condition checks times.
        if (this.isTimeReporterActive())
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_GLOBAL_TERMINATION_CONDITION_CHECK, startTime,
                    finishTime);

        // Returns the result.
        return isGlobalTerminationConditionSatisfied;
    }

    protected List<Pair<IndividualWrapper<Individual, FitnessValue>, IndividualWrapper<Individual, FitnessValue>>>
    runParentsSelection(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            ParentsSelection<Individual, FitnessValue> parentsSelectionClassInstance
    ) throws IOException {
        // Registers the parents selection start time.
        long startTime = System.currentTimeMillis();

        // Calls the selection operator.
        List<IndividualWrapper<Individual, FitnessValue>> populationCopy =
                new ArrayList<IndividualWrapper<Individual, FitnessValue>>(population);
        List<Pair<IndividualWrapper<Individual, FitnessValue>,
                IndividualWrapper<Individual, FitnessValue>>> selectedCouples =
                parentsSelectionClassInstance.selectParents(populationCopy);

        // Registers the selection finish time.
        long finishTime = System.currentTimeMillis();

        // Writes the parents selection time.
        if (this.isTimeReporterActive())
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_PARENTS_SELECTION, startTime, finishTime);

        // Returns the result.
        return selectedCouples;
    }

    protected List<IndividualWrapper<Individual, FitnessValue>> runCrossover(
            List<Pair<IndividualWrapper<Individual, FitnessValue>, IndividualWrapper<Individual, FitnessValue>>>
                    selectedCouples,
            Crossover<Individual, FitnessValue> crossoverClassInstance
    ) throws IOException {
        // Instantiates the offspring population list.
        List<IndividualWrapper<Individual, FitnessValue>> offspringPopulation =
                new ArrayList<IndividualWrapper<Individual, FitnessValue>>();

        // Iterates among the selected couples.
        int currentCoupleNumber = 0;

        long currentStartTime;
        long currentFinishTime;
        long totalCrossoverTime = 0L;
        long numberOfCrossovers = 0L;

        for (Pair<IndividualWrapper<Individual, FitnessValue>, IndividualWrapper<Individual, FitnessValue>>
                currentCouple : selectedCouples) {
            // Reads the parents from the couple.
            IndividualWrapper<Individual, FitnessValue> currentParentIndividual1 = currentCouple.getFirstElement();
            IndividualWrapper<Individual, FitnessValue> currentParentIndividual2 = currentCouple.getSecondElement();

            // Registers the current crossover start time.
            currentStartTime = System.currentTimeMillis();

            // Calls the crossover operator.
            List<IndividualWrapper<Individual, FitnessValue>> currentChildren =
                    crossoverClassInstance.cross(currentParentIndividual1, currentParentIndividual2,
                            currentCoupleNumber, selectedCouples.size(), this.inputPopulation.size());

            // Registers the current crossover finish time.
            currentFinishTime = System.currentTimeMillis();

            // Adds the individual to the list.
            offspringPopulation.addAll(currentChildren);

            // Updates the times.
            totalCrossoverTime += currentFinishTime - currentStartTime;
            numberOfCrossovers++;

            // Increments the couple number.
            currentCoupleNumber++;
        }

        // Writes the crossover times.
        if (this.isTimeReporterActive()) {
            long averageCrossoverTime = (numberOfCrossovers != 0L)
                    ? (long) Math.ceil((double) totalCrossoverTime / (double) numberOfCrossovers) : 0L;
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.AVERAGE_CROSSOVER, 0L, averageCrossoverTime);
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_CROSSOVER, 0L, totalCrossoverTime);
        }

        // Returns the result.
        return offspringPopulation;
    }

    protected void runMutation(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            Mutation<Individual, FitnessValue> mutationClassInstance
    ) throws IOException {
        // Iterates among the individuals.
        long currentStartTime;
        long currentFinishTime;
        long totalMutationTime = 0L;
        long numberOfMutations = 0L;

        for (int currentIndividualIndex = 0; currentIndividualIndex < population.size(); currentIndividualIndex++) {
            // Extracts the current individual.
            IndividualWrapper<Individual, FitnessValue> currentIndividual = population.get(currentIndividualIndex);

            // Registers the current mutation start time.
            currentStartTime = System.currentTimeMillis();

            // Calls the mutation operator.
            population.set(currentIndividualIndex, mutationClassInstance.mutate(currentIndividual));

            // Registers the current mutation finish time.
            currentFinishTime = System.currentTimeMillis();

            // Updates the times.
            totalMutationTime += currentFinishTime - currentStartTime;
            numberOfMutations++;
        }

        // Writes the mutation times.
        if (this.isTimeReporterActive()) {
            long averageMutationTime = (numberOfMutations != 0L) ?
                    (long) Math.ceil((double) totalMutationTime / (double) numberOfMutations) : 0L;
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.AVERAGE_MUTATION, 0L, averageMutationTime);
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_MUTATION, 0L, totalMutationTime);
        }
    }

    protected void runOffspringFitnessEvaluation(
            List<IndividualWrapper<Individual, FitnessValue>> offspringPopulation,
            FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance
    ) throws IOException {

        long currentFitnessEvaluationStartTime;
        long currentFitnessEvaluationFinishTime;
        long currentFitnessEvaluationTime = 0L;
        long minFitnessEvaluationTime = Long.MAX_VALUE;
        long maxFitnessEvaluationTime = Long.MIN_VALUE;
        long totalFitnessEvaluationTime = 0L;
        long numberOfFitnessEvaluations = 0L;

        // Computes the fitness values for the offspring individuals.
        for (IndividualWrapper<Individual, FitnessValue> currentIndividual : offspringPopulation) {
            if (!currentIndividual.isFitnessValueSet()) {
                // Registers the current fitness evaluation start time.
                currentFitnessEvaluationStartTime = System.currentTimeMillis();

                // Calls the operator.
                FitnessValue currentFitnessValue = fitnessEvaluationClassInstance.evaluate(currentIndividual);
                currentIndividual.setFitnessValue(currentFitnessValue);

                // Registers the current fitness evaluation finish time.
                currentFitnessEvaluationFinishTime = System.currentTimeMillis();

                // Updates the times.
                currentFitnessEvaluationTime = currentFitnessEvaluationFinishTime - currentFitnessEvaluationStartTime;
                if (currentFitnessEvaluationTime < minFitnessEvaluationTime)
                    minFitnessEvaluationTime = currentFitnessEvaluationTime;
                if (currentFitnessEvaluationTime > maxFitnessEvaluationTime)
                    maxFitnessEvaluationTime = currentFitnessEvaluationTime;
                totalFitnessEvaluationTime += currentFitnessEvaluationTime;
                numberOfFitnessEvaluations++;
            }
        }

        // Writes the survival selection time.
        if (this.isTimeReporterActive()) {
            if (numberOfFitnessEvaluations == 0L) {
                minFitnessEvaluationTime = 0;
                maxFitnessEvaluationTime = 0;
            }
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.MIN_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION, 0L,
                    minFitnessEvaluationTime);
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.MAX_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION, 0L,
                    maxFitnessEvaluationTime);
            long averageFitnessEvaluationTime = (numberOfFitnessEvaluations != 0L)
                    ? (long) Math.ceil((double) totalFitnessEvaluationTime / (double) numberOfFitnessEvaluations) : 0L;
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.AVERAGE_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION, 0L,
                    averageFitnessEvaluationTime);
            this.geneticOperatorsTimeReporter.writeTime(this.nodeNumber, this.generationsBlockNumber,
                    this.currentGenerationNumber, GeneticOperatorsTimeReporter.PhaseType.TOTAL_FITNESS_EVALUATION_DURING_SURVIVAL_SELECTION, 0L,
                    totalFitnessEvaluationTime);
        }
    }

    protected List<IndividualWrapper<Individual, FitnessValue>>
    runSurvivalSelection(
            List<IndividualWrapper<Individual, FitnessValue>> parentsPopulation,
            List<IndividualWrapper<Individual, FitnessValue>> offspringPopulation,
            SurvivalSelection<Individual, FitnessValue> survivalSelectionClass,
            FitnessEvaluation<Individual, FitnessValue> fitnessEvaluationClassInstance
    ) throws IOException {

        // Registers the survival selection start time.
        long survivalSelectionStartTime = System.currentTimeMillis();

        // Makes the copies of populations.
        List<IndividualWrapper<Individual, FitnessValue>> parentsPopulationCopy = new ArrayList<>(parentsPopulation);
        List<IndividualWrapper<Individual, FitnessValue>> offspringPopulationCopy = new ArrayList<>(offspringPopulation);

        // Calls the survival selection operator and sets the new population.
        List<IndividualWrapper<Individual, FitnessValue>> newPopulation =
                survivalSelectionClass.selectSurvivors(parentsPopulationCopy, offspringPopulationCopy);

        // Registers the survival selection finish time.
        long survivalSelectionFinishTime = System.currentTimeMillis();

        // Writes the survival selection time.
        if (this.isTimeReporterActive()) {
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_SURVIVAL_SELECTION, survivalSelectionStartTime, survivalSelectionFinishTime);
        }

        // Returns the result.
        return newPopulation;
    }

    protected List<IndividualWrapper<Individual, FitnessValue>> runElitism(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            Elitism<Individual, FitnessValue> elitismClassInstance
    ) throws IOException {
        // Registers the elitism start time.
        long startTime = System.currentTimeMillis();

        ArrayList<IndividualWrapper<Individual, FitnessValue>> populationCopy =
                new ArrayList<>(population);

        // Calls the elitism operator and sets the elite population.
        List<IndividualWrapper<Individual, FitnessValue>> elitePopulation =
                elitismClassInstance.selectElite(populationCopy);

        // Registers the elitism finish time.
        long finishTime = System.currentTimeMillis();

        // Writes the elitism time.
        if (this.isTimeReporterActive())
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_ELITISM, startTime, finishTime);

        // Returns the result.
        return elitePopulation;
    }

    protected List<Pair<IndividualWrapper<Individual, FitnessValue>, Integer>> runMigration(
            List<IndividualWrapper<Individual, FitnessValue>> population,
            Migration<Individual, FitnessValue> migrationClassInstance
    ) throws IOException {
        // Registers the Migration start time.
        long startTime = System.currentTimeMillis();

        // Calls the migration operator.
        List<Pair<IndividualWrapper<Individual, FitnessValue>, Integer>> migrationAssignements =
                migrationClassInstance.assign(population);

        // Registers the migration finish time.
        long finishTime = System.currentTimeMillis();

        // Writes the migration time.
        if (this.isTimeReporterActive())
            this.geneticOperatorsTimeReporter.writeTime(
                    this.nodeNumber, this.generationsBlockNumber, this.currentGenerationNumber,
                    GeneticOperatorsTimeReporter.PhaseType.TOTAL_MIGRATION, startTime, finishTime);

        // Returns the result.
        return migrationAssignements;
    }

    public void filterSolutions() {
        this.solutionsPopulation = new ArrayList<IndividualWrapper<Individual, FitnessValue>>();
        this.nonsolutionsPopulation = new ArrayList<IndividualWrapper<Individual, FitnessValue>>();

        for (IndividualWrapper<Individual, FitnessValue> currentIndividualWrapper : this.outputPopulation) {
            if (currentIndividualWrapper.isTerminationConditionSatisfied()) {
                this.solutionsPopulation.add(currentIndividualWrapper);
            } else {
                this.nonsolutionsPopulation.add(currentIndividualWrapper);
            }
        }
    }

    public boolean checkTerminationConditionSatisfactionNotifications() {
        return false;
    }

    public void notifyTerminationConditionSatisfaction() {
    }

    public boolean shareIslandProperties(Properties properties, long generationNumber) {
        return true;
    }

    public List<Properties> readSharedIslandProperties(Properties islandProperties, long generationNumber) {
        List<Properties> result = new ArrayList<Properties>();
        result.add(islandProperties);
        return result;
    }
}
