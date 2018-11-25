// Program to find the total number of distinct triangles present in the given data using Reduce Side Join
package wc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class CountFollowers extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CountFollowers.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private final Text follower = new Text();
		private final Text followee = new Text();

		// Map function to get followers for each user
		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(","); // split line by comma and save userids in array
			if (tokens.length > 0 && Integer.parseInt(tokens[0]) <= 80000 && Integer.parseInt(tokens[1]) <= 80000) {
				follower.set(tokens[0]);
				followee.set("From," + tokens[1]);
				context.write(follower, followee); // emit the edges in the given order and mark them as From edges
				follower.set("To," + tokens[0]);
				followee.set(tokens[1]);
				context.write(followee, follower); // emit the edges in reverse order and mark them as To edges

				/*
				 * We emit edges in reverse direction also so that the keys remain same and the
				 * mapper automatically sends same keyrecords to the same reducer. hence we do
				 * not have to write special partitioner to implement this.
				 */
			}
		}
	}

	public static class ReduceSideJoin extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> listTo = new ArrayList<Text>();
		private ArrayList<Text> listFrom = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Clear our lists
			listTo.clear();
			listFrom.clear();

			// Store From and To edges in different List
			for (Text t : values) {
				String[] tokens = t.toString().split(",");
				if (tokens[0].equals("To")) {
					listTo.add(new Text(tokens[1]));
				} else if (tokens[0].equals("From")) {
					listFrom.add(new Text(tokens[1]));
				}
			}

			executeJoinLogic(context);
		}

		private void executeJoinLogic(Context context) throws IOException, InterruptedException {

			// Identifies paths of length two
			if (!listTo.isEmpty() && !listFrom.isEmpty()) {
				for (Text A : listTo) {
					for (Text B : listFrom) {
						context.write(A, B); // emit the missing edge which can make the path closed
					}
				}
			}
		}
	}

	public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {

		private final Text outkey = new Text();
		private final Text outvalue = new Text();

		// Read the input file again and emit reverse or the To edges
		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(","); // split line by comma and save userids in array
			if (tokens.length > 0 && Integer.parseInt(tokens[0]) <= 80000 && Integer.parseInt(tokens[0]) <= 80000) {
				outkey.set(tokens[1] + "," + tokens[0]);
				outvalue.set("To");
				context.write(outkey, outvalue);
			}
		}
	}

	public static class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Read the intermediate result and emit the From edges
			String line = value.toString();
			outkey.set(line);
			outvalue.set("From");
			context.write(outkey, outvalue);
		}
	}

	public static class finalReducer extends Reducer<Text, Text, Text, Text> {

		private ArrayList<Text> listTo = new ArrayList<Text>();
		private ArrayList<Text> listFrom = new ArrayList<Text>();
		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Clear our lists
			listTo.clear();
			listFrom.clear();

			// Store From and To edges in different lists
			for (Text t : values) {
				if (t.toString().equals("To")) {
					listTo.add(new Text(key));
				} else if (t.toString().equals("From")) {
					listFrom.add(new Text(key));
				}
			}

			executeJoinLogic(context);
		}

		// Join the edges
		private void executeJoinLogic(Context context) throws IOException, InterruptedException {
			if (!listTo.isEmpty() && !listFrom.isEmpty()) {
				for (Text A : listFrom) {
					for (Text B : listTo) {
						context.write(A,B); // assigning same to to all entries to enable
																			// count functionality
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Followers Count");
		job.setJarByClass(CountFollowers.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		// initial phase to read the input file and emit From and To edges
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ReduceSideJoin.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		// Delete output directory, only to ease local development; will not work on
		// AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================

		final Job nextJob = Job.getInstance(conf, "Reduce side join");

		// Intermediate phase to find the common vertices and join the data. We use
		// inner Join
		nextJob.setJarByClass(CountFollowers.class);
		nextJob.setReducerClass(finalReducer.class);

		nextJob.setOutputKeyClass(Text.class);
		nextJob.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(nextJob, new Path(args[0]), TextInputFormat.class, UserJoinMapper.class);
		MultipleInputs.addInputPath(nextJob, new Path(args[1]), TextInputFormat.class, CommentJoinMapper.class);
		FileOutputFormat.setOutputPath(nextJob, new Path(args[2]));
		job.waitForCompletion(true);

		return nextJob.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <intermediate-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new CountFollowers(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
