package PageRank;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;

public class PageRankJob{
	private static final Path tmpInputPath = new Path(PageRankSetting.TMP_INPUT_PATH);
	private static final Path tmpOutputPath = new Path(PageRankSetting.TMP_OUTPUT_PATH);
	public static int main(String args[]) throws Exception{

		if (args.length < 3 ){
			System.out.println("ERROR: At least 3 parameters are needed, <OUTPUT_PATH> <BETA> <TOTAL_NODE_COUNT>");
			return 1;
		}
		// Create a new Job
		Configuration conf = new Configuration();

		moveOutputFiletoInputFile(conf);

		conf.setDouble(PageRankSetting.CONF_KEY_BETA, Double.parseDouble(args[1]));
		conf.setInt(PageRankSetting.CONF_KEY_TOTAL_NODE_COUNT, Integer.parseInt(args[2]) );

		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRankJob.class);

		// Specify various job-specific parameters
		job.setJobName("PageRankJob");

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		job.setCombinerClass(PageRankReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RankAndAdjacentNodesPair.class);

		FileInputFormat.addInputPath(job, tmpInputPath);
		FileOutputFormat.setOutputPath(job, tmpOutputPath);

		// Submit the job, then poll for progress until the job is complete
		if (!job.waitForCompletion(true))
		{
			return 1;
		}

		return sortPageRank( new Path(args[0]) );
	}

	private static int sortPageRank(Path outputPath) throws Exception{
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRankJob.class);

		// Specify various job-specific parameters
		job.setJobName("SortPageRankJob");

		job.setMapperClass(SortPageRankMapper.class);
		job.setReducerClass(SortPageRankReducer.class);

		job.setOutputKeyClass(PageRankNodeIdPair.class);
		job.setOutputValueClass(NullWritable.class);

		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(outputPath)){
			fileSystem.delete(outputPath,true);
		}
		fileSystem.close();
		FileInputFormat.addInputPath(job, tmpOutputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true)?0:1;
	}

	private static void moveOutputFiletoInputFile(Configuration conf) throws Exception{
		FileSystem fileSystem = FileSystem.get(conf);
		if ( !fileSystem.exists( tmpOutputPath) ){
			throw new IOException("ERROR: output path ("+tmpOutputPath+") doesn't exist!");
		}

		if ( fileSystem.exists(tmpInputPath) ){
			fileSystem.delete(tmpInputPath, true);
		}
		fileSystem.rename( tmpOutputPath , tmpInputPath);
		fileSystem.close();
	}
}