package it.unisa.elephant56.core.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

import it.unisa.elephant56.core.Constants;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes Avro records to an Avro container file output stream with a meta data of number of records.
 *
 * @param <K> The (java) type of the Avro data to write
 * @param <V> The (java) type of value data to write
 */
public class PopulationRecordWriter<K, V> extends RecordWriter<AvroKey<K>, V> {

    private final CodecFactory compressionCodec;
    private ArrayList<AvroKey<K>> recordsList;
    private Schema writerSchema;
    private OutputStream outputStream;

    // Record counter.
    private int numberOfRecords;

    /**
     * Constructor.
     *
     * @param writerSchema     The writer schema for the records in the Avro container file.
     * @param compressionCodec A compression codec factory for the Avro container file.
     * @param outputStream     The output stream to write the Avro container file to.
     * @throws IOException If the record writer cannot be opened.
     */
    public PopulationRecordWriter(Schema writerSchema, CodecFactory compressionCodec, OutputStream outputStream)
            throws IOException {
        this.compressionCodec = compressionCodec;
        this.writerSchema = writerSchema;
        this.outputStream = outputStream;
        this.recordsList = new ArrayList<AvroKey<K>>();

        // Initialises the counter.
        this.numberOfRecords = 0;
    }

    @Override
    public void write(AvroKey<K> key, V value)
            throws IOException {
        // Clones the key.
        AvroKey<K> keyClone = new AvroKey<K>(key.datum());

        // Add the record to the list.
        this.recordsList.add(keyClone);

        // Increments the counter.
        this.numberOfRecords++;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        // Create an Avro container file and a writer to it.
        DataFileWriter<K> avroFileWriter;
        avroFileWriter = new DataFileWriter<K>(new ReflectDatumWriter<K>(writerSchema));
        avroFileWriter.setCodec(compressionCodec);

        // Writes the meta-data.
        avroFileWriter.setMeta(Constants.AVRO_NUMBER_OF_RECORDS, this.numberOfRecords);

        // Writes the file.
        avroFileWriter.create(this.writerSchema, this.outputStream);
        for (AvroKey<K> record : this.recordsList)
            avroFileWriter.append(record.datum());

        // Close the stream.
        avroFileWriter.close();
    }
}
