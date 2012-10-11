
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class CVSPairThreshold extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();

			// Split the input line into an artist list of Strings
			String artists[] = line.split(",");

			// Pair up the artists selected by a user for subsequent occurance counting
			for (int i = 0; i < artists.length-1; i++) {
				for (int j = i+1; j < artists.length; j++) {
					int cmp = artists[i].compareTo(artists[j]);
					String artist1;
					String artist2;

					// Keep the artist pairs sorted lexicographically
					if ( cmp == 0 ) 
						// Skip any repeated artist pair in an input line
						continue;
					else if ( cmp < 0 ) {
						artist1 = artists[j];
						artist2 = artists[i];
					} else {
						artist1 = artists[i];
						artist2 = artists[j];
					}

					// Combine the paired artists into a combined text key with an occurance count here of one
					word.set(artist1+","+artist2);
					output.collect(word, one);
				}
			}
		}
	}

	// Provide a local combiner for reduction of local paired output counts
	public static class MyCombiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	// Create a reducer that outputs just the key if (THRESHOLD_COUNT) or more occurances of an artist pair is found.
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, NullWritable> {

		private static final int THRESHOLD_COUNT = 2;
		private static final NullWritable nullw = NullWritable.get();

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if ( sum >= THRESHOLD_COUNT ) 
				output.collect(key, nullw);
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CVSPairThreshold.class);
		conf.setJobName("CVSPairThreshold");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(MyCombiner.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if ( args.length < 2 ) {
			System.out.println("CVSPairThreshout usage:  hadoop jar cvs.jar CVSPairThreshold <hdfs input directory> <hdfs output directory>");
			System.exit(-1);
		}
		int res = ToolRunner.run(new Configuration(), new CVSPairThreshold(), args);
		System.exit(res);
	}
}
