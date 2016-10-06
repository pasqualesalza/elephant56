package it.unisa.elephant56.core.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import it.unisa.elephant56.core.Constants;
import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;
import it.unisa.elephant56.util.hadoop.FilesPathFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;

/**
 * Defines an input format for Avro object that associates a file to a specific node,
 * according to the location on the HDFS. It can also producing empty split in case of
 * initialisation.
 *
 * @param <Type> the type of Avro object to serialise
 */
public class NodesInputFormat<Type> extends InputFormat<AvroKey<Type>, IntWritable> {
    public static String NUMBER_OF_NODES =
            "configuration.nodes_input_format.number_of_nodes.int";

    public static String INPUT_POPULATION_FOLDER_PATH =
            "configuration.nodes_input_format.input_population_folder_path.string";
    public static String IS_INITIALISATION_ACTIVE =
            "configuration.nodes_input_format.is_initialisation_active.boolean";
    public static String INITIALISATION_POPULATION_SIZE_PER_SPLIT =
            "configuration.nodes_input_format.initialisation_population_size_per_split.int";

    public static String PARTITIONER_NODES_DESTINATIONS =
            "configuration.nodes_input_format.partitioner_nodes_destinations.string";

    public static final FilesPathFilter avroFilesPathFilter = new FilesPathFilter(null, Constants.AVRO_FILE_EXTENSION);

    /**
     * Sets the number of nodes.
     *
     * @param job           the job to configure
     * @param numberOfNodes the number of nodes
     */
    public static void setNumberOfNodes(Job job, int numberOfNodes) {
        Configuration configuration = job.getConfiguration();
        configuration.setInt(NUMBER_OF_NODES, numberOfNodes);
    }

    /**
     * Retrieves the number of nodes.
     *
     * @param job the job to configure
     * @return the number of nodes
     */
    public static int getNumberOfNodes(Job job) {
        Configuration configuration = job.getConfiguration();
        return configuration.getInt(NUMBER_OF_NODES, 0);
    }

    /**
     * Sets the input population folder path.
     *
     * @param job        the job to configure
     * @param folderPath the input population folder path
     */
    public static void setInputPopulationFolderPath(Job job, Path folderPath) {
        Configuration configuration = job.getConfiguration();
        configuration.set(INPUT_POPULATION_FOLDER_PATH, folderPath.toString());
    }

    /**
     * Retrieves the input population folder path.
     *
     * @param job the job to configure
     * @return the input population folder path
     */
    public static Path getInputPopulationFolderPath(Job job) {
        Configuration configuration = job.getConfiguration();
        return new Path(configuration.get(INPUT_POPULATION_FOLDER_PATH, ""));
    }

    /**
     * Sets the number of individuals to generate during the initialisation.
     *
     * @param job  the job to configure
     * @param size the number of individuals to generate
     */
    public static void setInitialisationPopulationSizePerSplit(Job job, int size) {
        Configuration configuration = job.getConfiguration();
        configuration.setInt(INITIALISATION_POPULATION_SIZE_PER_SPLIT, size);
    }

    /**
     * Retrieves the number of individuals to generate during the initialisation.
     *
     * @param job the job to configure
     * @return the number of individuals to generate
     */
    public static int getInitialisationPopulationSize(Job job) {
        Configuration configuration = job.getConfiguration();
        return configuration.getInt(INITIALISATION_POPULATION_SIZE_PER_SPLIT, 0);
    }

    /**
     * Activates the initialisation and the production of empty splits.
     *
     * @param job    the job to configure
     * @param active "true" to activate, "false" otherwise
     */
    public static void activateInitialisation(Job job, boolean active) {
        Configuration configuration = job.getConfiguration();
        configuration.setBoolean(IS_INITIALISATION_ACTIVE, active);
    }

    /**
     * Sets the partitioner destinations of individuals.
     *
     * @param job          the job to configure
     * @param destinations the list of destinations
     */
    public static void setPartitionerNodesDestinations(Job job, List<Integer> destinations) {
        Configuration configuration = job.getConfiguration();
        String string = PopulationInputSplit.convertIntegersListToString(destinations);
        configuration.set(PARTITIONER_NODES_DESTINATIONS, string);
    }


    /**
     * Gets the partitioner destinations of individuals.
     *
     * @param configuration the configuration to parse
     * @return the list of destinations or null
     */
    public static List<Integer> getPartitionerNodesDestinations(Configuration configuration) {
        String string = configuration.get(PARTITIONER_NODES_DESTINATIONS);
        List<Integer> partitionerNodesDestinations = PopulationInputSplit.convertStringToIntegersList(string);
        return partitionerNodesDestinations;
    }

    /**
     * Retrieves if the initialisation is active.
     *
     * @param job the job to configure
     * @return "true" if active, "false" otherwise
     */
    public static boolean isInitialisationActive(Job job) {
        Configuration configuration = job.getConfiguration();
        return configuration.getBoolean(IS_INITIALISATION_ACTIVE, false);
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        // Gets the configuration.
        Configuration configuration = jobContext.getConfiguration();

        // Retrieves the file system.
        FileSystem fileSystem = FileSystem.get(configuration);

        // Retrieves the properties.
        int numberOfNodes = configuration.getInt(NUMBER_OF_NODES, 0);
        Path inputPopulationFolderPath = new Path(configuration.get(INPUT_POPULATION_FOLDER_PATH, "input"));
        boolean isInitialisationActive = configuration.getBoolean(IS_INITIALISATION_ACTIVE, false);
        int initialisationPopulationSizePerSplit = configuration.getInt(INITIALISATION_POPULATION_SIZE_PER_SPLIT, 0);
        List<Integer> partitionerNodesDestinations = getPartitionerNodesDestinations(configuration);

        // Retrieves the Avro schema.
        Schema schema = AvroJob.getInputKeySchema(configuration);

        // Constructs the array of InputSplit.
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // Checks if the initialisation is active.
        if (isInitialisationActive) {
            // Generates the input splits.
            int currentFromIndividualIndex = 0;
            int currentToIndividualIndex = initialisationPopulationSizePerSplit;
            for (int i = 0; i < numberOfNodes; i++) {
                // TODO Locations non funziona.
                //String[] locations = new String[] {Integer.toString(i)};
                String[] locations = new String[0];

                List<Integer> splitPartitionerNodesDestinations = null;
                if (partitionerNodesDestinations != null)
                    splitPartitionerNodesDestinations = partitionerNodesDestinations.subList(currentFromIndividualIndex, currentToIndividualIndex);

                splits.add(new PopulationInputSplit(true, inputPopulationFolderPath, initialisationPopulationSizePerSplit,
                        splitPartitionerNodesDestinations, locations));

                if (partitionerNodesDestinations != null) {
                    currentFromIndividualIndex = currentToIndividualIndex;
                    currentToIndividualIndex += initialisationPopulationSizePerSplit;
                    if (currentToIndividualIndex > partitionerNodesDestinations.size())
                        currentToIndividualIndex = partitionerNodesDestinations.size();
                }
            }
        } else {
            // Retrieves the files status.
            FileStatus[] files = fileSystem.listStatus(inputPopulationFolderPath, avroFilesPathFilter);

            int currentFromIndividualIndex = 0;
            for (int i = 0; i < numberOfNodes; i++) {
                FileStatus file = files[i];
                Path filePath = file.getPath();
                int numberOfIndividuals = readNumberOfIndividuals(filePath, configuration);

                // TODO Locations non funziona.
                //String[] locations = fileSystem.getFileBlockLocations(file, 0L, 1L)[0].getHosts();
                String[] locations = new String[0];

                int currentToIndividualIndex = currentFromIndividualIndex + numberOfIndividuals;
                List<Integer> splitPartitionerNodesDestinations = null;
                if (partitionerNodesDestinations != null)
                    splitPartitionerNodesDestinations = partitionerNodesDestinations.subList(currentFromIndividualIndex, currentToIndividualIndex);

                splits.add(new PopulationInputSplit(false, filePath, numberOfIndividuals, splitPartitionerNodesDestinations,
                        locations));

                if (partitionerNodesDestinations != null) {
                    currentFromIndividualIndex = currentToIndividualIndex;
                }
            }
        }


        return splits;
    }

    /**
     * Retrieves a new record reader.
     */
    @Override
    public RecordReader<AvroKey<Type>, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new PopulationRecordReader<>();
    }

    /**
     * Reads the number of individuals into a file from meta-data.
     *
     * @param filePath      the file path
     * @param configuration the configuration
     * @return the number of individuals
     * @throws IOException
     */
    public static int readNumberOfIndividuals(Path filePath, Configuration configuration) throws IOException {
        SeekableInput seekableFileInput = new FsInput(filePath, configuration);
        ReflectData reflectData = new ReflectData(configuration.getClassLoader());
        DatumReader<IndividualWrapper<Individual, FitnessValue>> datumReader =
                new ReflectDatumReader<IndividualWrapper<Individual, FitnessValue>>(reflectData);
        DataFileReader<IndividualWrapper<Individual, FitnessValue>> avroFileReader =
                new DataFileReader<IndividualWrapper<Individual, FitnessValue>>(seekableFileInput, datumReader);

        int result = (int) avroFileReader.getMetaLong(Constants.AVRO_NUMBER_OF_RECORDS);

        avroFileReader.close();

        return result;
    }
}
