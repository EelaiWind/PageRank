package PageRank;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class SortPageRankReducer extends Reducer<PageRankNodeIdPair, NullWritable, PageRankNodeIdPair, NullWritable>{
	private int printedNodeCount;
	private final static int topN = 10;
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		printedNodeCount = 0;
	}

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		try {
			while (context.nextKey()) {
				if ( printedNodeCount < topN ){
					reduce(context.getCurrentKey(), context.getValues(), context);
				}
				else{
					break;
				}
			}
		} finally {
			cleanup(context);
		}
	}


	@Override
	public void reduce(PageRankNodeIdPair key,  Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
		printedNodeCount += 1;
	}
}