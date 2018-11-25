// Program to calculate the total number of followers for each user
package wc;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
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

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text userid = new Text();

        // Map function to get followers for each user
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(","); // split line by comma and save userids in array
            if (tokens[1] != null) {
                userid.set(tokens[1]); // emit 1 for userid being followed
                context.write(userid, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        // Reduce function to get the total count of followers for each user by grouping userid
        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            Counter minCounter = context.getCounter(constants.MIN);
            Counter maxCounter = context.getCounter(constants.MAX);
            if (maxCounter.getValue() < sum) {
                maxCounter.setValue(sum);
            }
            if ((minCounter.getValue() > sum) || (minCounter.getValue() == 0)) {
                minCounter.setValue(sum);
            }
            context.write(key, result);
        }
    }

    public static class ClusterMapper extends Mapper<Object, Text, LongWritable, LongWritable> {

        public List<Long> centroids = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            String centroidFilePath = context.getConfiguration().get("centroidFilePath");
            URI s3uri=URI.create(centroidFilePath);
            FileSystem fs=FileSystem.get(s3uri,new Configuration());
            Path path = new Path(centroidFilePath);
            FileStatus[] valFilePathList = fs.listStatus(path);
            for (FileStatus f : valFilePathList) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] tokens = line.split("\\t");
                    centroids.add(Long.parseLong(tokens[0]));
                }
            }

        }

        // Map function to assign clusters
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String line = value.toString();
            long minDist = Long.MAX_VALUE;
            long centroidChoosen = centroids.get(0);

            String[] tokens = line.split("\\t");
            long followersCount = Long.parseLong(tokens[1]);

            for (int i = 0; i < centroids.size(); i++) {
                long dist = euclideanDistance(centroids.get(i), followersCount);
                if (minDist > dist) {
                    centroidChoosen = centroids.get(i);
                    minDist = dist;
                }
            }

            context.write(new LongWritable(centroidChoosen), new LongWritable(followersCount));

        }
    }


    public static class ClusterReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            long sum = 0;
            List<Long> items = new ArrayList<>();

            for (LongWritable val : values) {
                long v = val.get();
                items.add(v);
                sum = sum + v;
                counter++;

            }

            float average = sum / counter;
            context.write(new LongWritable((long)average), new LongWritable(counter));

            Counter sseCounter = context.getCounter(constants.SSE);
            double sse = sseCounter.getValue();

            for (long i:items){
                sse = sse + Math.pow((average - i),2);
            }

            sseCounter.setValue((long)sse);

        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Followers Count");
        job.setJarByClass(CountFollowers.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/final-input-data"));

        job.waitForCompletion(true);

        // create random centroid file
        int numberOfClusters = Integer.parseInt(args[2]);
        List<Long> centroids = createGoodCentroids(numberOfClusters, job.getCounters().findCounter(constants.MIN).getValue(), job.getCounters().findCounter(constants.MAX).getValue());

        String stringCentroid = getCentroidString(centroids);

        // write the centroids in a file
        writeCentroids(conf, stringCentroid, args[1]);

        // ********************************* Next job ************************************
        Configuration conf2 = getConf();
        int iteration = 0;
        int result = 0;
        boolean hasConverged = false;

        while (!hasConverged && iteration < 10) {
            conf2.set("centroidFilePath", args[1] + "/final-output-data/Iteration-" + iteration);
            iteration++;

            final Job nextJob = Job.getInstance(conf2, "Iteration Count");
            nextJob.setMapperClass(ClusterMapper.class);
            nextJob.setReducerClass(ClusterReducer.class);
            nextJob.setOutputKeyClass(LongWritable.class);
            nextJob.setOutputValueClass(LongWritable.class);
            nextJob.setJarByClass(CountFollowers.class);
            FileInputFormat.addInputPath(nextJob, new Path(args[1] + "/final-input-data"));
            String outputPath = args[1] + "/final-output-data/Iteration-" + iteration;
            FileOutputFormat.setOutputPath(nextJob, new Path(outputPath));
            result = nextJob.waitForCompletion(true) ? 0 : 1;

            List<Long> previousCentroids = getCentroids(conf2, conf2.get("centroidFilePath"));
            List<Long> newCentroids = getCentroids(conf2, outputPath);


            if (previousCentroids.equals(newCentroids)) {
                hasConverged = true;
            }

        }

        return result;
    }


    public enum constants {
        MIN, MAX, SSE
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir> <cluster-count>");
        }

        try {
            ToolRunner.run(new CountFollowers(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    public static List<Long> createGoodCentroids(int k, long min, long max) {

        // distribute the centroid evenly
        List<Long> centroids = new ArrayList<>();
        long firstCentroid = max/k;
        centroids.add(firstCentroid);

        for (int j = 2; j <= k; j++) {
            centroids.add(firstCentroid*j);
        }

        return centroids;
    }


    /*public static List<Long> createBadCentroids(int k, long min, long max) {

        // distribute the centroid evenly
        List<Long> centroids = new ArrayList<>();
        for (int j = 0; j < k; j++) {
            long c = (long)Math.pow(10,j);
            centroids.add(c);
        }

        return centroids;
    }*/

    public static void writeCentroids(Configuration conf, String formattedCentroids, String path) throws IOException {

        URI s3uri=URI.create(path);
        FileSystem fs=FileSystem.get(s3uri,conf);
        FSDataOutputStream fin = fs.create(new Path(path + "/final-output-data/Iteration-0/default"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
        bw.append(formattedCentroids.toString());
        bw.close();
    }


    public static String getCentroidString(List<Long> centroids) {
        StringBuilder s = new StringBuilder();

        for (Long c : centroids) {
            s.append(c);
            s.append("\n");
        }

        return s.toString();
    }


    public static long euclideanDistance(long centroid, long coordinate) {
        return Math.abs(centroid - coordinate);
    }

    public List<Long> getCentroids(Configuration conf, String path) throws IOException {

        List<Long> result = new ArrayList<>();

        URI s3uri=URI.create(path);
        FileSystem fs=FileSystem.get(s3uri,conf);
        Path p = new Path(path);
        FileStatus[] valFilePathList = fs.listStatus(p);
        for (FileStatus f : valFilePathList) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
            String line;
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\\t");
                result.add(Long.parseLong(tokens[0]));
            }

            br.close();

        }
        return result;
    }

}