package amazonEmotion;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.hackreduce.mappers.ModelMapper;

public abstract class AmazonMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<AmazonRecord, LongWritable, Text, K, V> {

	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected AmazonRecord instantiateModel(LongWritable key, Text value) {
		String inputString = value.toString();
		
		return new AmazonRecord(inputString);
	}

}

