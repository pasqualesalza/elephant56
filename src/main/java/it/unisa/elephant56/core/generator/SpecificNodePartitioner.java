package it.unisa.elephant56.core.generator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.avro.mapred.AvroKey;

import it.unisa.elephant56.core.common.IndividualWrapper;
import it.unisa.elephant56.user.common.FitnessValue;
import it.unisa.elephant56.user.common.Individual;

/**
 * Associates the island number requested to the respective reduce task number.
 * It works only if the number of map tasks and the number reduce tasks are the same.
 */
public class SpecificNodePartitioner
		extends Partitioner<AvroKey<IndividualWrapper<Individual, FitnessValue>>, IntWritable> {
	/**
	 * Associates the island number requested to the same reduce task number.
	 */
	@Override
	public int getPartition(
            AvroKey<IndividualWrapper<Individual, FitnessValue>> key,
            IntWritable value,
            int numPartitions) {
		return value.get();
	}
}
