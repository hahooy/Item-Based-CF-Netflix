package job0;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* Preprocess Training data, precompute numRatings and sumRatings for every movie.
 * Input (MovieId, UserId, Rating)
 * (UserId,MovieId,Rating;numRatings;sumRatings) is the output of this job */

public class TrainingDataPreprocessDriver extends Configured implements Tool {

	private static Logger THE_LOGGER = Logger
			.getLogger(TrainingDataPreprocessDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(TrainingDataPreprocessDriver.class);
		job.setJobName("TrainingDataPreprocess");


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TrainingDataPreprocessMapper.class);
		job.setReducerClass(TrainingDataPreprocessReducer.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);
		THE_LOGGER.info("run(): status=" + status);
		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// Make sure there are exactly 2 parameters
		if (args.length < 2) {
			THE_LOGGER
					.warn("usage TrainingDataPreprocessDriver <input> <output>");
			System.exit(1);
		}

		THE_LOGGER.info("inputDir=" + args[0]);
		THE_LOGGER.info("outputDir=" + args[1]);
		int returnStatus = ToolRunner.run(new TrainingDataPreprocessDriver(),
				args);
		System.exit(returnStatus);
	}

}
