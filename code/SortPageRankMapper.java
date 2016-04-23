package PageRank;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashSet;

public class SortPageRankMapper extends Mapper<LongWritable, Text, PageRankNodeIdPair, NullWritable>{
	private PageRankNodeIdPair outputKey = new PageRankNodeIdPair();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] tokens = value.toString().split("\\s+");
		int nodeId = Integer.parseInt(tokens[0]);
		double pageRank = Double.parseDouble(tokens[1]);

		outputKey.setPageRank(pageRank);
		outputKey.setNodeId(nodeId);
		context.write(outputKey, NullWritable.get());
	}
}