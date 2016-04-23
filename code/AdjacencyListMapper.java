package PageRank;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AdjacencyListMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	IntWritable outputKey = new IntWritable();
	Text outputValue = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] token = value.toString().split("\\s");

		// skip comment
		if (value.toString().charAt(0) == '#'){
			return;
		}

		outputKey.set(Integer.parseInt(token[0]));
		outputValue.set(token[1]);
		context.write(outputKey, outputValue);
	}
}