

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class MaxFollowers.
 */
public class MaxFollowers extends Configured implements Tool {
	
		/** The Constant logger. */
		private static final Logger logger = LogManager.getLogger(MaxFollowers.class);

		/**
		 * The Class TokenizerMapper.
		 */
		public static class TokenizerMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		
			/** The word. */
			private final LongWritable word = new LongWritable();
			
			/** The Constant one. */
			private final static LongWritable one = new LongWritable(1);

			/** The max foll. */
			long maxFoll = 0;

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
			public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
				
				String[] tokens = value.toString().split(",");
				
				if(Long.parseLong(tokens[1]) > maxFoll)
				{
				maxFoll = Long.parseLong(tokens[1]);
				}
				
				word.set(maxFoll);
				context.write(one, word);
			}
		}

		/**
		 * The Class IntSumReducer.
		 */
		public static class IntSumReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
			
			/** The max foll. */
			long maxFoll = 0;
			
			/** The result. */
			private final LongWritable result = new LongWritable();

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
			public void reduce(final LongWritable key, final Iterable<LongWritable> values, final Context context) throws IOException, InterruptedException {
			
				 int k = context.getConfiguration().getInt("k", 0);

				for (final LongWritable val : values)
					{
						if(val.get() > maxFoll)
						{
							maxFoll = val.get();
						}
					}
					
			    	
					result.set(maxFoll);
					context.write(key,result);
					
				
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
			final Job job = Job.getInstance(conf, "Max followers");
			job.setJarByClass(MaxFollowers.class);
			
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", ",");
			jobConf.set("k", args[2]);
			
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(LongWritable.class);
			
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
		
			if (args.length != 3) {
				throw new Error("Two arguments required:\n<input-dir> <output-dir>");
			}

			try {
			
				ToolRunner.run(new MaxFollowers(), args);
			}
			catch (final Exception e) {
			
				logger.error("", e);
			}
		}
}
