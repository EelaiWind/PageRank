package PageRank;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;

public class DeadEndRankMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
	private final static IntWritable outputKey = new IntWritable(1);
	private DoubleWritable outputValue = new DoubleWritable();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] tokens = value.toString().split("\\s+");
		double rank = Double.parseDouble(tokens[1]);

		if ( tokens.length == 2 ){
			outputValue.set(rank);
			context.write(outputKey,outputValue);
		}
	}
}