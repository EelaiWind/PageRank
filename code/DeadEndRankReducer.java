package PageRank;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class DeadEndRankReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{
	private DoubleWritable outputValue = new DoubleWritable();
	private double beta;
	private int totalNodeCount;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		beta = conf.getDouble(PageRankSetting.CONF_KEY_BETA, -1);
		totalNodeCount = conf.getInt(PageRankSetting.CONF_KEY_TOTAL_NODE_COUNT, -1);
		if ( beta == -1){
			throw new IOException("ERROR: the vale of beta is not defined");
		}

		if ( totalNodeCount == -1 ){
			throw new IOException("ERROR: total number of nodes is not defined");
		}
	}

	@Override
	public void reduce(IntWritable key,  Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		double totalDeadEndRank = 0;

		for (DoubleWritable rank : values){
			totalDeadEndRank += rank.get();
		}
		totalDeadEndRank = beta*totalDeadEndRank/totalNodeCount;
		outputValue.set(totalDeadEndRank);
		context.write(key, outputValue);
	}
}