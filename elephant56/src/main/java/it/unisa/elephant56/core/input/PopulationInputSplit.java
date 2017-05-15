package it.unisa.elephant56.core.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.StringUtils;

/**
 * Defines an input split of Avro objects.
 */
public class PopulationInputSplit extends InputSplit implements Writable {

    private boolean isInitialisationActive;
    private Path filePath;
    private int numberOfIndividuals;
    private List<Integer> partitionerNodesDestinations;

    private String[] locations;

    /**
     * Constructs an empty input split.
     */
    public PopulationInputSplit() {
    }

    /**
     * Constructs the input split.
     *
     * @param isInitialisationActive       "true" if initialisation is active, "false" otherwise
     * @param filePath                     the path of the file
     * @param numberOfIndividuals          the number of individuals in the split
     * @param partitionerNodesDestinations the nodes destinations of the individuals
     * @param locations                    the locations of the individuals into HDFS
     */
    public PopulationInputSplit(boolean isInitialisationActive, Path filePath, int numberOfIndividuals, List<Integer> partitionerNodesDestinations, String[] locations) {
        this.isInitialisationActive = isInitialisationActive;
        this.filePath = filePath;
        this.numberOfIndividuals = numberOfIndividuals;
        this.partitionerNodesDestinations = partitionerNodesDestinations;

        this.locations = locations;
    }

    /**
     * Retrieves the file path.
     *
     * @return the file path
     */
    public Path getFilePath() {
        return this.filePath;
    }

    /**
     * Serialises the split.
     */
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeBoolean(this.isInitialisationActive);

        String filePathString = "";
        if (!this.isInitialisationActive)
            filePathString = this.filePath.toString();
        Text.writeString(output, filePathString);

        output.writeInt(this.numberOfIndividuals);

        String partitionerNodesDestinationsString =
                convertIntegersListToString(this.partitionerNodesDestinations);
        Text.writeString(output, partitionerNodesDestinationsString);
    }

    /**
     * Deserialises the split.
     */
    @Override
    public void readFields(DataInput input) throws IOException {
        this.isInitialisationActive = input.readBoolean();

        String filePathString = Text.readString(input);

        if (!this.isInitialisationActive)
            this.filePath = new Path(filePathString);

        this.numberOfIndividuals = input.readInt();

        String partitionerNodesDestinationsString = Text.readString(input);
        this.partitionerNodesDestinations = convertStringToIntegersList(partitionerNodesDestinationsString);
    }

    /**
     * Returns the number of Avro objects inside the split.
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.numberOfIndividuals;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return this.locations;
    }

    public List<Integer> getPartitionerNodesDestinations() {
        return this.partitionerNodesDestinations;
    }

    /**
     * Returns if the split is empty.
     *
     * @return "true" if the split is empty, "false" otherwise
     */
    public boolean isInitialisationActive() {
        return this.isInitialisationActive;
    }

    public static String convertIntegersListToString(List<Integer> integersList) {
        if ((integersList == null) || (integersList.size() == 0))
            return "";

        String[] strings = new String[integersList.size()];
        for (int i = 0; i < integersList.size(); i++) {
            Integer currentInteger = integersList.get(i);
            strings[i] = Integer.toString(currentInteger);
        }

        return StringUtils.arrayToString(strings);
    }

    public static List<Integer> convertStringToIntegersList(String string) {
        String[] strings = StringUtils.getStrings(string);

        if ((strings == null) || (strings.length == 0))
            return null;

        List<Integer> integersList = new ArrayList<>(strings.length);
        for (int i = 0; i < strings.length; i++) {
            Integer currentInteger = Integer.parseInt(strings[i]);
            integersList.add(i, currentInteger);
        }
        return integersList;
    }
}
