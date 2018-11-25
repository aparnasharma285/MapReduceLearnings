// Program to calculate the total number of followers for each user
package wc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CountFollowers extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(CountFollowers.class);

    static enum MoreIterations {
        numUpdated
    }

    public static class AdjacencyListCreatorMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split(","); // split line by comma
            context.write(new Text(tokens[0]), new Text(tokens[1]));


        }
    }

    public static class AdjacencyListCreatorReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            StringBuffer adjList = new StringBuffer();

            for (Text v : values) {
                adjList.append(v.toString() + ",");
            }

            context.write(key, adjList.length() > 0 ? new Text(adjList.substring(0, adjList.length() - 1)) : new Text(""));
        }
    }


    public static class DistanceCalculatorMapper extends Mapper<Object, Text, Text, Text> {

        private static final String dummyNode = "null";

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String source = conf.get("source");
            String line = value.toString();
            String[] nodes = line.split("\\s+");

            // check if the node has adjacency list exists
            if (nodes.length < 2) {
                return;
            }

            String currentNode = nodes[0];
            String zero = Long.toString(0);
            String currentActive = source;

            long distance = Long.MAX_VALUE;
            Text n = new Text(currentNode);
            Text N = new Text();

            if (nodes.length == 2) {
                if (currentNode.equals(source)) {
                    distance = 0;
                    N.set(String.join(" ", nodes[1], zero, currentNode));
                } else {
                    N.set(String.join(" ", nodes[1], Long.toString(Long.MAX_VALUE), dummyNode));
                }
            } else {
                distance = Long.parseLong(nodes[2]);
                currentActive = nodes[3];
                N.set(String.join(" ", nodes[1], nodes[2], nodes[3]));
            }
            context.write(n, N);


            for (String neighbour : nodes[1].split(",")) {

                String pathFromSource = "";
                if (neighbour.equals(dummyNode)) {
                    return;
                }
                n.set(neighbour);
                long newDistance = distance;
                System.out.println("DISTANCE"+distance);
                if (newDistance != Long.MAX_VALUE) {
                    System.out.println("EMITTING");
                    newDistance += 1;
                    if (!currentActive.equals(currentNode)) {
                        pathFromSource = currentNode + "," + currentActive;
                    } else {
                        pathFromSource = source;
                    }

                    N.set(String.join(" ", Long.toString(newDistance),
                            pathFromSource));
                    context.write(n, N);
                }
            }
        }
    }


    public static class DistanceCalculatorReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            Text result = new Text();
            long minDistance = Long.MAX_VALUE;
            long existingDistance = Long.MAX_VALUE;
            String newPath = "";
            String existingPath = "";
            String adjacentNode = null;


            for (Text v : values) {
                String[]  list = v.toString().split("\\s+");



                if (list.length == 2) {

                    long dis = Long.parseLong(list[0]);

                    if (dis < minDistance) {
                        minDistance = dis;
                        newPath = list[1];
                    }

                } else {
                    existingDistance = Long.parseLong(list[1]);
                    adjacentNode = list[0];
                    existingPath = list[2];
                }
            }

            long newDistance = existingDistance;
            String updatedPath = existingPath;

            if (minDistance < existingDistance) {
                context.getCounter(CountFollowers.MoreIterations.numUpdated).increment(1);
                newDistance = minDistance;
                updatedPath = newPath;
                System.out.println("NEEDS REITERATION");

            }

            result.set(String.join(" ", adjacentNode, Long.toString(newDistance), updatedPath));
            context.write(key, result);
        }

    }


    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Followers Count");
        job.setJarByClass(CountFollowers.class);
        Configuration jobConf = job.getConfiguration();

        job.getConfiguration().set("source", args[3]);

        job.setMapperClass(AdjacencyListCreatorMapper.class);
        job.setReducerClass(AdjacencyListCreatorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        // Next Job


        long numUpdated = 1;
        int code = 0;
        int numIterations = 1;

        while (numUpdated > 0) {
            String input, output;
            final Job nextJob = Job.getInstance(conf, "Shortest Path");

            nextJob.getConfiguration().set("source", args[3]);
            if (numIterations == 1) {
                input = args[1];
            } else {
                input = args[2] + "-" + (numIterations - 1);
            }
            output = args[2] + "-" + numIterations;


            nextJob.setJarByClass(CountFollowers.class);
            nextJob.setMapperClass(DistanceCalculatorMapper.class);
            nextJob.setReducerClass(DistanceCalculatorReducer.class);

            nextJob.setOutputKeyClass(Text.class);
            nextJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(nextJob, new Path(input));
            FileOutputFormat.setOutputPath(nextJob, new Path(output));
            code = nextJob.waitForCompletion(true) ? 0 : 1;

            Counters jobCounters = nextJob.getCounters();
            numUpdated = jobCounters.findCounter(MoreIterations.numUpdated).getValue();
            numIterations += 1;
        }
        return code;
        }

        public static void main ( final String[] args){
            if (args.length != 4) {
                throw new Error("Three arguments required:\n<input-dir> <intermediate-dir> <output-dir> source");
            }

            try {
                ToolRunner.run(new CountFollowers(), args);
            } catch (final Exception e) {
                logger.error("", e);
            }
        }

    }
