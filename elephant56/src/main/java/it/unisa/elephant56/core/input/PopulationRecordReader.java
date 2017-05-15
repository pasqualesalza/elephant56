package it.unisa.elephant56.core.input;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;

/**
 * Defines the reader of Avro input splits, that transforms these splits
 * in Avro objects.
 *
 * @param <Type> the type of Avro object to serialise
 */
public class PopulationRecordReader<Type> extends RecordReader<AvroKey<Type>, IntWritable> {

    private PopulationInputSplit currentSplit;

    private Configuration configuration;

    private DataFileReader<Type> avroFileReader;

    private List<Integer> partitionerNodesDestinations;

    private int currentRecordNumber;

    private Type currentDatum;
    private AvroKey<Type> currentKey;

    private IntWritable currentValue;

    /**
     * Constructs a new reader.
     */
    public PopulationRecordReader() {
        this.currentSplit = null;
        this.configuration = null;

        this.avroFileReader = null;

        this.currentRecordNumber = 0;
        this.currentDatum = null;
        this.currentKey = null;
        this.currentValue = null;
    }

    /**
     * Initialises the reader.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        // Checks if the split is compatible.
        if (!(split instanceof PopulationInputSplit))
            throw new IllegalArgumentException("Only compatible with PopulationInputSplits.");

        PopulationInputSplit populationSplit = (PopulationInputSplit) split;

        // Prepares the fields.
        this.currentSplit = populationSplit;
        this.configuration = context.getConfiguration();
        this.currentRecordNumber = 0;
        this.currentDatum = null;
        this.currentKey = new AvroKey<Type>(null);
        this.currentValue = new IntWritable();

        // Initialises the file reader.
        if (!populationSplit.isInitialisationActive()) {
            SeekableInput seekableFileInput = new FsInput(this.currentSplit.getFilePath(), this.configuration);
            ReflectData reflectData = new ReflectData(this.configuration.getClassLoader());
            DatumReader<Type> datumReader = new ReflectDatumReader<Type>(reflectData);
            this.avroFileReader = new DataFileReader<Type>(seekableFileInput, datumReader);
        }

        // Initialises the partitioner nodes destinations list.
        this.partitionerNodesDestinations = populationSplit.getPartitionerNodesDestinations();
    }

    /**
     * Checks if a new Avro object is available and shifts to it.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Checks if there are not individuals.
        if (this.currentSplit.isInitialisationActive() && (this.partitionerNodesDestinations == null))
            return false;

        // If not empty split.
        if (!this.currentSplit.isInitialisationActive()) {
            assert this.avroFileReader != null;

            // If all the objects have been read, return false.
            if (!this.avroFileReader.hasNext())
                return false;

            // Reads the next Avro object.
            this.currentDatum = avroFileReader.next();

            // Stores the Avro object in a AvroKey container and returns true.
            this.currentKey.datum(this.currentDatum);
        }

        // Reads the next value, if present.
        if (this.partitionerNodesDestinations != null) {
            // If all the objects have been read, return false.
            if (currentRecordNumber >= this.partitionerNodesDestinations.size())
                return false;

            currentValue.set(this.partitionerNodesDestinations.get(currentRecordNumber));
        }

        // Increments the record number.
        this.currentRecordNumber++;

        return true;
    }

    /**
     * Returns the current key.
     */
    @Override
    public AvroKey<Type> getCurrentKey() throws IOException, InterruptedException {
        return this.currentKey;
    }

    /**
     * Returns the current value.
     */
    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return this.currentValue;
    }

    /**
     * Returns the progress of the reading.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (this.currentSplit.isInitialisationActive())
            return 1.0f;

        assert this.avroFileReader != null;

        if (this.currentSplit.getLength() == 0)
            return 0.0f;

        return Math.min(1.0f, ((float) currentRecordNumber / (float) this.currentSplit.getLength()));
    }

    /**
     * Closes the streams.
     */
    @Override
    public void close() throws IOException {
        if (this.avroFileReader != null) {
            try {
                this.avroFileReader.close();
            } finally {
                this.avroFileReader = null;
            }
        }
    }
}
