package it.unisa.elephant56.core.output;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroOutputFormatBase;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * FileOutputFormat for writing Avro container files with a metadata of number of records.
 * 
 * Since Avro container files only contain records (not key/value pairs), this output format ignores the value.
 *
 * @param <K> The (java) type of the Avro data to write
 * @param <V> The (java) type of value data to write
 */
public class NodesOutputFormat<K, V> extends
		AvroOutputFormatBase<AvroKey<K>, V> {

	// A factory for creating record writers.
	private final RecordWriterFactory recordWriterFactory;

	/**
	 * Constructor.
	 */
	public NodesOutputFormat() {
		this(new RecordWriterFactory<K, V>());
	}

	/**
	 * Constructor.
	 *
	 * @param recordWriterFactory A factory for creating record writers.
	 */
	protected NodesOutputFormat(RecordWriterFactory<K, V> recordWriterFactory) {
		this.recordWriterFactory = recordWriterFactory;
	}

	/**
	 * A factory for creating record writers.
	 *
	 * @param <K> The java type of the avro record to write
	 * @param <V> The (java) type of value data to write
	 */
	protected static class RecordWriterFactory<K, V> {
		/**
		 * Creates a new record writer instance.
		 *
		 * @param writerSchema The writer schema for the records to write.
		 * @param compressionCodec The compression type for the writer file.
		 * @param outputStream The target output stream for the records.
		 */
		protected RecordWriter<AvroKey<K>, V> create(Schema writerSchema, CodecFactory compressionCodec, OutputStream outputStream)
				throws IOException {
			return new PopulationRecordWriter<K, V>(writerSchema, compressionCodec, outputStream);
		}
		
	}

	@Override
	@SuppressWarnings("unchecked")
	public RecordWriter<AvroKey<K>, V> getRecordWriter(TaskAttemptContext context)
			throws IOException {
		// Get the writer schema.
	    Schema writerSchema = AvroJob.getOutputKeySchema(context.getConfiguration());
	    
	    if (null == writerSchema)
	    	throw new IOException(NodesOutputFormat.class.getName() + " requires an output schema. Use AvroJob.setOutputKeySchema().");

	    return this.recordWriterFactory.create(writerSchema, getCompressionCodec(context),
                getAvroFileOutputStream(context));
	}


}
