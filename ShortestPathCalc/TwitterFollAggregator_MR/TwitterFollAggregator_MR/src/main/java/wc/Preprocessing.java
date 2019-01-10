package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class Preprocessing.
 */
public class Preprocessing extends Configured implements Tool {
	
		/** The Constant logger. */
		private static final Logger logger = LogManager.getLogger(TwitterShortestPath.class);

		/**
		 * Mapper Class which contains the map function called on each input record
		 * Extracts the userId of the user being followed as key, assigns a count value one 
		 * for each occurrence and emits the list of key value pairs.  
		 * **/
		public static class PreProcessingMapper extends Mapper<Object, Text, Text, Text> {
		
			
			/** The word. */
			private final Text word = new Text();

			/**
			 * Map.
			 *
			 * @param key the key
			 * @param value the value
			 * @param context the context
			 * @throws IOException Signals that an I/O exception has occurred.
			 * @throws InterruptedException the interrupted exception
			 */
			/*
			 * map function runs on each input record. 
			 */
			@Override
			public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
				// Split the line of input record based on delimiter ","
				// obtained two values are stored in an Array of strings
				// extract the second value in the array which is the person followed
		
				    String line = value.toString();
		            String[] nodes = line.split(",");
		            Text source = new Text(nodes[0]);
		            Text dest = new Text(nodes[1]);
		            context.write(source, dest);
				
			}
		}

		/**
		 * The Class PreProcessingReducer.
		 */
		/*
		 * Reducer Class which contains the reduce function called on each unique key value pair 
		 * */
		public static class PreProcessingReducer extends Reducer<Text, Text, Text, Text> {
			
			/** The result. */
			private final IntWritable result = new IntWritable();
			
			
			/**
			 * Reduce.
			 *
			 * @param key the key
			 * @param values the values
			 * @param context the context
			 * @throws IOException Signals that an I/O exception has occurred.
			 * @throws InterruptedException the interrupted exception
			 */
			/*
			 * iterates through the individual values for each key and emits the (key, total). 
			 */
			@Override
			public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
				StringBuffer adjacencyList = new StringBuffer();
	            Text result = new Text();
	            
                adjacencyList.append("99999999");

                adjacencyList.append(" ");
                
                adjacencyList.append("false");
                
                adjacencyList.append(" ");


	            for (Text neighbour : values) {
	                adjacencyList.append(neighbour);
	                adjacencyList.append(":");
	            }
                adjacencyList.append(":");

	            String resultString =
	                adjacencyList.length() > 0 ?
	                adjacencyList.substring(0, adjacencyList.length() - 1) :
	                "";
	            result.set(resultString);
	            context.write(key, result);
			}
		}
		
		/*
		 * Driver code sets Mapper and Reducer class and controls program execution.
		 */

		/**
		 * Run.
		 *
		 * @param args the args
		 * @return the int
		 * @throws Exception the exception
		 */
		@Override
		public int run(final String[] args) throws Exception {
			
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "Preprocessor");
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", " ");
	        job.setJarByClass(Preprocessing.class);
	        job.setMapperClass(PreProcessingMapper.class);
	        job.setReducerClass(PreProcessingReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        return job.waitForCompletion(true) ? 0 : 1;
			
			
		}

		/**
		 * The main method.
		 *
		 * @param args the arguments
		 */
		public static void main(final String[] args) {
			
		
			if (args.length != 2) {
				throw new Error("Two arguments required:\n<input-dir> <output-dir>");
			}

			try {
			
				ToolRunner.run(new Preprocessing(), args);
			}
			catch (final Exception e) {
			
				logger.error("", e);
			}
		}
}