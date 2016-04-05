package it.unisa.elephant56.core;

/**
 * Defines the constants to be used by the core.
 */
public final class Constants {
    /**
     * The default lock file name.
     */
    public static final String LOCK_FILE_NAME =
            "lock";

    public static final long LOCK_TIME_TO_WAIT =
            100L;

    /**
     * The meta-data property for Avro files.
     */
    public static final String AVRO_NUMBER_OF_RECORDS =
            "number_of_records";

    /**
     * The file extension for the avro files.
     */
    public static final String AVRO_FILE_EXTENSION =
            "avro";

    /**
     * The file extension for the jar files.
     */
    public static final String JAR_FILE_EXTENSION =
            "jar";

    /**
     * The format of island names.
     */
    public static final String ISLANDS_NAME_FORMAT =
            "%05d";

    /**
     * The format of generation name.
     */
    public static final String CONFIGURATION_GENERATION_NAME_FORMAT =
            "elephant56.configuration.generation_name_format";

    /**
	 * The path of the folder where are the files representing the fact the termination condition has been satisfied at
     * least once.
	 */
	public static final String CONFIGURATION_TERMINATION_FLAG_FILES_FOLDER_PATH =
            "elephant56.configuration.termination_flag_files_folder.path";

    /**
     * The file extension for the termination flag files.
     */
    public static final String TERMINATION_FLAG_FILE_EXTENSION =
            "termination_flag";

	/**
	 * The path of the folder where the islands properties files are.
	 */
	public static final String CONFIGURATION_ISLAND_PROPERTIES_FILES_FOLDER_PATH =
            "elephant56.configuration.island_properties_files_folder.path";

    /**
     * The file extension for the properties files.
     */
    public static final String PROPERTIES_FILES_EXTENSION =
            "properties";

    /**
     * The start generation number.
     */
    public static final String CONFIGURATION_START_GENERATION_NUMBER =
            "elephant56.configuration.start_generation_number";

    /**
     * The finish generation number.
     */
    public static final String CONFIGURATION_FINISH_GENERATION_NUMBER =
            "elephant56.configuration.finish_generation_number";

    /**
     * The number of individual for the initialisation operator.
     */
    public static final String CONFIGURATION_INITIALISATION_POPULATION_SIZE =
            "elephant56.configuration.initialisation.population_size";

    /**
     * The flag that indicates if the initialisation is active or not.
     */
    public static final String CONFIGURATION_INITIALISATION_ACTIVE =
            "elephant56.configuration.initialisation.active";

    /**
     * The flag that indicates if the survival selection is active or not.
     */
    public static final String CONFIGURATION_SURVIVAL_SELECTION_ACTIVE =
            "elephant56.configuration.survival_selection.active";

	/**
	 * The flag that indicates if the elitism is active or not.
	 */
	public static final String CONFIGURATION_ELITISM_ACTIVE =
            "elephant56.configuration.elitism.active";
	
	/**
	 * The flag that indicates if the migration is active or not.
	 */
	public static final String CONFIGURATION_MIGRATION_ACTIVE =
            "elephant56.configuration.migration.active";
	
	/**
	 * The flag that indicates if the time reporter is active or not.
	 */
	public static final String CONFIGURATION_TIME_REPORTER_ACTIVE =
            "elephant56.configuration.reporter.time.active";
	
	/**
	 * The flag that indicates if the individual reporter is active or not.
	 */
	public static final String CONFIGURATION_INDIVIDUAL_REPORTER_ACTIVE =
            "elephant56.configuration.reporter.individual.active";
	
	/**
	 * The fitness value class configuration string.
	 */
	public static final String CONFIGURATION_FITNESS_VALUE_CLASS =
            "elephant56.configuration.fitness_value.class";
	
	/**
	 * The initialisation class configuration string.
	 */
	public static final String CONFIGURATION_INITIALISATION_CLASS =
            "elephant56.configuration.initialisation.class";
	
	/**
	 * The fitness evaluation class configuration string.
	 */
	public static final String CONFIGURATION_FITNESS_EVALUATION_CLASS =
            "elephant56.configuration.fitness_evaluation.class";
	
	/**
	 * The termination condition check class configuration string.
	 */
	public static final String CONFIGURATION_TERMINATION_CONDITION_CHECK_CLASS =
            "elephant56.configuration.termination_condition_check.class";
	
	/**
	 * The parents selection class configuration string.
	 */
	public static final String CONFIGURATION_PARENTS_SELECTION_CLASS =
            "elephant56.configuration.parents_selection.class";
	
	/**
	 * The crossover class configuration string.
	 */
	public static final String CONFIGURATION_CROSSOVER_CLASS =
            "elephant56.configuration.crossover.class";
	
	/**
	 * The mutation class configuration string.
	 */
	public static final String CONFIGURATION_MUTATION_CLASS =
            "elephant56.configuration.mutation.class";

    /**
     * The survival selection class configuration string.
     */
    public static final String CONFIGURATION_SURVIVAL_SELECTION_CLASS =
            "elephant56.configuration.survival_selection.class";

	/**
	 * The elitism class configuration string.
	 */
	public static final String CONFIGURATION_ELITISM_CLASS =
            "elephant56.configuration.elitism.class";
	
	/**
	 * The migration class configuration string.
	 */
	public static final String CONFIGURATION_MIGRATION_CLASS =
            "elephant56.configuration.migration.class";

    /**
     * The flag that indicates if the generation is the last for the grid parallelisation model.
     */
    public static final String CONFIGURATION_GRID_PARALLELISATION_MODEL_IS_LAST_GENERATION =
            "elephant56.configuration.grid_parallelisation_model.is_last_generation";

	/**
	 * The reports folder path.
	 */
	public static final String CONFIGURATION_REPORTS_FOLDER_PATH =
            "elephant56.configuration.reports_folder.path";

    public static final String CONFIGURATION_GENERATIONS_BLOCK_NUMBER =
            "elephant56.configuration.generations_block_number.long";

    public static final String GENETIC_OPERATORS_TIME_REPORT_FILE_NAME_FORMAT =
            "times-genetic_operators-i(" + ISLANDS_NAME_FORMAT + ").csv";

    public static final String GENERATIONS_BLOCK_TIME_REPORT_FILE_NAME_FORMAT =
            "times-generations_block-i(" + ISLANDS_NAME_FORMAT + ").csv";

    public static final String GRID_GENERATIONS_BLOCK_TIME_REPORT_FILE_NAME_FORMAT =
            "times-generations_block.csv";

    public static final String MAPREDUCE_TIME_REPORT_FILE_NAME_FORMAT =
            "times-mapreduce.csv";

    public static final String MAPREDUCE_MAPPER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT =
            "times-mapreduce-i(" + ISLANDS_NAME_FORMAT + ").csv.m.part";

    public static final String MAPREDUCE_REDUCER_PARTIAL_TIME_REPORT_FILE_NAME_FORMAT =
            "times-mapreduce-i(" + ISLANDS_NAME_FORMAT + ").csv.r.part";

    public static final String INDIVIDUAL_REPORT_FILE_NAME_FORMAT =
            "individuals-i(" + ISLANDS_NAME_FORMAT + ").csv";

    public static final String CONFIGURATION_POPULATION_NAME =
            "population";

    public static final String CONFIGURATION_SOLUTIONS_NAME =
            "solutions";

    public static final String CONFIGURATION_NONSOLUTIONS_NAME =
            "nonsolutions";

    /**
     * The default parents output folder name.
     */
    public static final String DEFAULT_PARENTS_OUTPUT_FOLDER_NAME =
            "parents";

    /**
     * The default offspring output folder name.
     */
    public static final String DEFAULT_OFFSPRING_OUTPUT_FOLDER_NAME =
            "offspring";

	/**
	 * The default input folder name.
	 */
	public static final String DEFAULT_INPUT_FOLDER_NAME =
            "input";
	
	/**
	 * The default output folder name.
	 */
	public static final String DEFAULT_OUTPUT_FOLDER_NAME =
            "output";
	
	/**
	 * The default generations folder name.
	 */
	public static final String DEFAULT_GENERATIONS_BLOCKS_FOLDER_NAME =
            "generations_blocks";
	
	/**
	 * The default termination flag files folder name.
	 */
	public static final String DEFAULT_TERMINATION_FLAG_FILES_FOLDER_NAME =
            "termination_flags";
	
	/**
	 * The default solutions folder name.
	 */
	public static final String DEFAULT_SOLUTIONS_FOLDER_NAME =
            "solutions";
	
	/**
	 * The default reports folder name.
	 */
	public static final String DEFAULT_REPORTS_FOLDER_NAME =
            "reports";

	/**
	 * The default islands properties files folder name.
	 */
	public static final String DEFAULT_ISLANDS_PROPERTIES_FILES_FOLDER_NAME =
            "islands_properties";

	private Constants() {
		throw new AssertionError();
	}
}
