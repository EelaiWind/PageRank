package PageRank;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashSet;

public class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, RankAndAdjacentNodesPair>{
	private IntWritable outputKey = new IntWritable();
	private RankAndAdjacentNodesPair outputValue = new RankAndAdjacentNodesPair();
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
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] tokens = value.toString().split("\\s+");
		int nodeId = Integer.parseInt(tokens[0]);
		double rank = Double.parseDouble(tokens[1]);
		HashSet<Integer> adjacentNodes = new HashSet<Integer>();
		for (int i = 2; i < tokens.length; i++){
			adjacentNodes.add(Integer.parseInt(tokens[i]));
		}
		writeAdjacencyList(context, nodeId, adjacentNodes);

		int sourceOutDegree = adjacentNodes.size();
		for (int adjacentNodeId : adjacentNodes){
			System.out.print(String.format("MYLOG: (%d, %d) = ",nodeId, adjacentNodeId));
			distributeRankToAdjacentNodes(context, adjacentNodeId, rank, sourceOutDegree);
		}
	}

	private void writeAdjacencyList(Context context, int nodeId, HashSet<Integer> adjacentNodes) throws IOException, InterruptedException{
		outputKey.set(nodeId);
		outputValue.setRank(0);
		outputValue.clearAdjacentNodes();
		outputValue.addAdjacentNodes(adjacentNodes);
		context.write(outputKey, outputValue);
	}

	private void distributeRankToAdjacentNodes(Context context, int adjacentNodeId, double sourceRank, int sourceOutDegree) throws IOException, InterruptedException{
		double rank = beta*(sourceRank/sourceOutDegree);

		outputKey.set(adjacentNodeId);
		outputValue.setRank(rank);
		outputValue.clearAdjacentNodes();
		System.out.println(String.format("%f (rank=%f)",rank,sourceRank));
		context.write(outputKey, outputValue);
	}
}