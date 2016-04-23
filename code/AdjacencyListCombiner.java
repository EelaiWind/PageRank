package PageRank;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AdjacencyListCombiner extends Reducer<IntWritable, Text, IntWritable, Text>{
	private Text outputValue = new Text();

	@Override
	public void reduce(IntWritable key,  Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String adjacentNodes = "";
		boolean isFirst = true;
		for (Text value : values){
			if ( isFirst ){
				isFirst = false;
				adjacentNodes += value.toString();
			}
			else{
				adjacentNodes += " " + value.toString();
			}
		}
		outputValue.set(adjacentNodes);
		context.write(key, outputValue);
	}

}