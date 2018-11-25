// Program to find the total number of distinct triangles present in the given data using Mapper Side Join
package wc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

public class CountFollowers extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CountFollowers.class);

	public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {
		HashMap<String, List<String>> followList = new HashMap<String, List<String>>(); // to store distributed cache data

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			try {
				URI[] cacheFiles = context.getCacheFiles();
				if (cacheFiles != null && cacheFiles.length > 0) {

					FileSystem fs = FileSystem.get(context.getConfiguration()); // read cache file line by line and create hashmap for quick access
					Path path = new Path(cacheFiles[0].toString());

					BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(path)));

					String line;
					// For each record in the user file
					while ((line = rdr.readLine()) != null) {
						String[] tokens = line.split(",");
						if (tokens.length > 1) {
							String key = tokens[0];
							String value = tokens[1];
							List<String> values = new ArrayList<String>();

							// Max filter applied
							if ((Integer.parseInt(key) <= 1000) && (Integer.parseInt(value) <= 1000)) {
								if (followList.containsKey(key)) {
									values = followList.get(key);

									if (!values.contains(value)) {
										values.add(value);
									}
								} else {
									values.add(value);
									followList.put(key, values);
								}
							}
						}
					}
					rdr.close();
				} else {
					throw new RuntimeException("User information is not set in DistributedCache");
				}

			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		// Map function to get followers for each user
		@Override
		public void map(final Object key, final Text value, final Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			// split line by comma and save userids in array
			String[] tokens = line.split(",");
			String userFrom = tokens[0];
			String userTo = tokens[1];

			// Applying Max filter
			if ((Integer.parseInt(userFrom) <= 1000) && (Integer.parseInt(userTo) <= 1000)) {
				
				// condition to check existence of triangle in the given data
				if (followList.containsKey(userTo) && followList.get(userTo).size() > 0) {
					List<String> values = followList.get(userTo);
					for (String v : values) {
						if (!(v.equals(userFrom)) && followList.containsKey(v) && followList.get(v).size() > 0) {
							List<String> followingUsers = followList.get(v);
							if (followingUsers.contains(userFrom)) {
								context.write(new Text("user"), new Text("one"));
							}

						}
					}
				}
			}
		}
	}

	public static class CountingReducer extends Reducer<Text, Text, Text, Text> {

		int count = 0;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text t : values) {
				count = count + 1; // to print total count of distinct triangles
			}

			count = count/3;
			context.write(new Text("Total Count"), new Text(Integer.toString(count)));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Followers Count");
		job.setJarByClass(CountFollowers.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		job.getConfiguration().set("join.type", "inner");

		// Delete output directory, only to ease local development; will not work on
		// AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================

		job.setMapperClass(ReplicatedJoinMapper.class);
		job.setReducerClass(CountingReducer.class);
		job.setNumReduceTasks(1);

		// Configure the DistributedCache
		job.addCacheFile(new Path(args[0] + "/edges.csv").toUri());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new CountFollowers(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
