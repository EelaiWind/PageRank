package PageRank;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class AdjacencyListJob{
	private static final Path tmpOutputPath = new Path(PageRankSetting.TMP_OUTPUT_PATH);

	public static int main(String args[]) throws Exception{

		if (args.length < 2 ){
			System.out.println("ERROR: At least 3 parameters are needed, <INPUT_PATH> <TOTAL_NODE_COUNT>");
			return 1;
		}
		// Create a new Job
		Configuration conf = new Configuration();
		conf.setInt(PageRankSetting.CONF_KEY_TOTAL_NODE_COUNT, Integer.parseInt(args[1]) );

		Job job = Job.getInstance(conf);
		job.setJarByClass(AdjacencyListJob.class);

		// Specify various job-specific parameters
		job.setJobName("AdjacencyListJob");

		job.setMapperClass(AdjacencyListMapper.class);
		job.setReducerClass(AdjacencyListReducer.class);
		job.setCombinerClass(AdjacencyListCombiner.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(tmpOutputPath)){
			fileSystem.delete(tmpOutputPath,true);
		}
		fileSystem.close();
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, tmpOutputPath);

		// Submit the job, then poll for progress until the job is complete
		return job.waitForCompletion(true)? 0: 1;
	}
}