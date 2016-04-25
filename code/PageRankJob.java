package PageRank;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class PageRankJob{
	private static final Path tmpInputPath = new Path(PageRankSetting.TMP_INPUT_PATH);
	private static final Path tmpOutputPath = new Path(PageRankSetting.TMP_OUTPUT_PATH);
	public static int main(String args[]) throws Exception{
		int iteration = 1;
		if (args.length < 4 ){
			System.out.println("ERROR: At least 4 parameters are needed, <OUTPUT_PATH> <BETA> <TOTAL_NODE_COUNT> <TRANK_SUM> <ITERATION>");
			return 1;
		}
		if ( args.length >= 5){
			try{
				iteration = Integer.parseInt(args[4]);
			}
			catch (Exception e){

			}
		}
		Configuration conf = new Configuration();
		conf.setDouble(PageRankSetting.CONF_KEY_BETA, Double.parseDouble(args[1]));
		conf.setInt(PageRankSetting.CONF_KEY_TOTAL_NODE_COUNT, Integer.parseInt(args[2]) );
		conf.setDouble(PageRankSetting.CONF_KEY_RANK_SUM, Double.parseDouble(args[3]) );

		for ( int i = 0 ; i < iteration; i++){
			moveOutputFiletoInputFile(conf);
			calculateDeadEndRank(conf);
			conf.setDouble(PageRankSetting.CONF_KEY_DEAD_END_RANK, getDeadEndRank(conf) );
			calculatePageRank(conf);
		}
		sortPageRank(conf, new Path(args[0]) );
		return 0;
	}

	private static int calculateDeadEndRank(Configuration conf) throws Exception{
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRankJob.class);

		// Specify various job-specific parameters
		job.setJobName("DeadEndRankJob");

		job.setMapperClass(DeadEndRankMapper.class);
		job.setReducerClass(DeadEndRankReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		deletePathIfExist(conf, tmpOutputPath);
		FileInputFormat.addInputPath(job, tmpInputPath);
		FileOutputFormat.setOutputPath(job, tmpOutputPath);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static double getDeadEndRank(Configuration conf) throws IOException{
		double deadEndRank;
		FileSystem fileSystem = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(
			new InputStreamReader(fileSystem.open(fileSystem.listStatus(tmpOutputPath)[1].getPath()))
		);
		deadEndRank = Double.parseDouble(reader.readLine().split("\\s")[1]);
		reader.close();
		fileSystem.close();

		return deadEndRank;
	}

	private static int calculatePageRank(Configuration conf) throws Exception{
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRankJob.class);

		// Specify various job-specific parameters
		job.setJobName("PageRankJob");

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setNumReduceTasks(10);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(RankAndAdjacentNodesPair.class);

		deletePathIfExist(conf, tmpOutputPath);
		FileInputFormat.addInputPath(job, tmpInputPath);
		FileOutputFormat.setOutputPath(job, tmpOutputPath);

		// Submit the job, then poll for progress until the job is complete
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private static int sortPageRank(Configuration conf, Path outputPath) throws Exception{
		Job job = Job.getInstance(conf);
		job.setJarByClass(PageRankJob.class);

		// Specify various job-specific parameters
		job.setJobName("SortPageRankJob");

		job.setMapperClass(SortPageRankMapper.class);
		job.setReducerClass(SortPageRankReducer.class);

		job.setOutputKeyClass(PageRankNodeIdPair.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(1);

		deletePathIfExist(conf, outputPath);
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

	private static void deletePathIfExist(Configuration conf, Path path) throws IOException{
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(path)){
			fileSystem.delete(path,true);
		}
		fileSystem.close();
	}
}