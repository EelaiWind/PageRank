package PageRank;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class AdjacencyListReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	private IntWritable outputKey = new IntWritable();
	private Text outputValue = new Text();
	private int totalNodeCount;
	private int printedNodeIndex;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		totalNodeCount = conf.getInt(PageRankSetting.CONF_KEY_TOTAL_NODE_COUNT, -1);
		if ( totalNodeCount == -1){
			throw new IOException("ERROR: total number of nodes is not defined");
		}
		printedNodeIndex = -1;
	}

	@Override
	public void reduce(IntWritable key,  Iterable<Text> values, Context context) throws IOException, InterruptedException {
		while ( printedNodeIndex < key.get()-1 ){
			printedNodeIndex += 1;
			outputKey.set(printedNodeIndex);
			outputValue.set("1.0");
			context.write(outputKey, outputValue);
		}

		String adjacentNodes = "1.0";
		for (Text value : values){
			adjacentNodes += " " + value.toString();
		}
		outputValue.set(adjacentNodes);
		context.write(key, outputValue);
		printedNodeIndex += 1;
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		while ( printedNodeIndex < totalNodeCount-1 ){
			printedNodeIndex += 1;
			outputKey.set(printedNodeIndex);
			outputValue.set("1.0");
			context.write(outputKey, outputValue);
		}
	}
}