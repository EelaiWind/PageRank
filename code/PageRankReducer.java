package PageRank;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class PageRankReducer extends Reducer<IntWritable, RankAndAdjacentNodesPair, IntWritable, RankAndAdjacentNodesPair>{
	RankAndAdjacentNodesPair outputValue = new RankAndAdjacentNodesPair();
	private double beta;
	private int totalNodeCount;
	private double rankOffset;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		double totalRankSum, deadEndRank;

		beta = conf.getDouble(PageRankSetting.CONF_KEY_BETA, -1);
		totalNodeCount = conf.getInt(PageRankSetting.CONF_KEY_TOTAL_NODE_COUNT, -1);
		totalRankSum = conf.getDouble(PageRankSetting.CONF_KEY_RANK_SUM, -1);
		deadEndRank = conf.getDouble(PageRankSetting.CONF_KEY_DEAD_END_RANK, -1);
		if ( beta == -1){
			throw new IOException("ERROR: the vale of beta is not defined");
		}
		if ( totalNodeCount == -1 ){
			throw new IOException("ERROR: total number of nodes is not defined");
		}
		if ( totalRankSum == -1){
			throw new IOException("ERROR: sum of all rank is not defined");
		}
		if ( deadEndRank == -1){
			throw new IOException("ERROR: dead-end rank is not defined");
		}
		rankOffset = deadEndRank + (1-beta)*totalRankSum/totalNodeCount;
		System.out.println("MYLOG: rankOffset = "+rankOffset);
	}

	@Override
	public void reduce(IntWritable key,  Iterable<RankAndAdjacentNodesPair> values, Context context) throws IOException, InterruptedException {
		double rankSum = 0;;
		outputValue.clearAdjacentNodes();
		System.out.println("MYLOG: Reducer Key = "+key);
		for (RankAndAdjacentNodesPair rankAndAdjacentNods : values){
			rankSum += rankAndAdjacentNods.getRank();
			System.out.println("MYLOG: rank = "+rankAndAdjacentNods.getRank());
			for ( Integer adjacentNodeId : rankAndAdjacentNods.getAdjacentNodes()){
				outputValue.addAdjacentNode(adjacentNodeId);
			}
		}
		rankSum += rankOffset;
		outputValue.setRank(rankSum);
		context.write(key, outputValue);
	}
}