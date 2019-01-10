package wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class Preprocessing.
 */
public class Preprocessing extends Configured implements Tool {
	
		/** The Constant logger. */
		private static final Logger logger = LogManager.getLogger(Preprocessing.class);

		/**
		 * The Class TokenizerMapper.
		 */
		public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
			/** The Constant one. */
			private final static IntWritable one = new IntWritable(1);
			
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
			@Override
			public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
				
				String[] tokens = value.toString().split(",");
				word.set(tokens[1]);
				context.write(word, one);
			}
		}

		/**
		 * The Class IntSumReducer.
		 */
		public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			
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
			@Override
			public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			
				int sum = 0;
				for (final IntWritable val : values)
				{
					sum += val.get();
				}
				
				result.set(sum);
				context.write(key, result);
				
			}
		}

		/**
		 * Run.
		 *
		 * @param args the args
		 * @return the int
		 * @throws Exception the exception
		 */
		@Override
		public int run(final String[] args) throws Exception {
		
			final Configuration conf = getConf();
			final Job job = Job.getInstance(conf, "Preprocessing");
			job.setJarByClass(Preprocessing.class);
			
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", ",");
			
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
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