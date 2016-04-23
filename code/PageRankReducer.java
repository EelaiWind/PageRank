package PageRank;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class PageRankReducer extends Reducer<IntWritable, RankAndAdjacentNodesPair, IntWritable, RankAndAdjacentNodesPair>{
	RankAndAdjacentNodesPair outputValue = new RankAndAdjacentNodesPair();


	@Override
	public void reduce(IntWritable key,  Iterable<RankAndAdjacentNodesPair> values, Context context) throws IOException, InterruptedException {
		double totalRank = 0;;
		outputValue.clearAdjacentNodes();
		for (RankAndAdjacentNodesPair rankAndAdjacentNods : values){
			totalRank += rankAndAdjacentNods.getRank();
			for ( Integer adjacentNodeId : rankAndAdjacentNods.getAdjacentNodes()){
				outputValue.addAdjacentNode(adjacentNodeId);
			}
		}
		outputValue.setRank(totalRank);
		context.write(key, outputValue);
	}
}