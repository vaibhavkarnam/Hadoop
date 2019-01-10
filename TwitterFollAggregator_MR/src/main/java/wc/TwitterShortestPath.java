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
 * The Class TwitterShortestPath.
 */
public class TwitterShortestPath extends Configured implements Tool {
	
		/** The Constant logger. */
		private static final Logger logger = LogManager.getLogger(TwitterShortestPath.class);

		/**
		 * Mapper Class which contains the map function called on each input record
		 * *.
		 */
		public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		
			
			/** The word. */
			private final Text word = new Text();

			/* (non-Javadoc)
			 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
			 */
			/*
			 * map function runs on each input record. 
			 */
			@Override
			public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
				// Split the line of input record based on delimiter " "
							
				 String line = value.toString();
				 
				 String[] lineSplit = line.split(" ");
				 int distanceadd = Integer.parseInt(lineSplit[1]) + 1;
				 String distance;

				 
				 String[] connectingNodes = lineSplit[3].split(":");
				 
				 for(int i=0; i<connectingNodes.length; i++)
				 {
					 
					 word.set("DIST "+distanceadd);
					 context.write(new Text(connectingNodes[i]), word);
					 word.clear();
				 }
				 
				 String isActive = lineSplit[2];
				 
				// System.out.println(isActive);
				 
				 if(isActive.equals("true"))
				 {
					 for(int i=0; i<connectingNodes.length; i++)
					 {
						 word.set("FLAG "+"true");
						 context.write( new Text(connectingNodes[i]), word);
						 word.clear();
					// System.out.println((connectingNodes[i])+"true");
					 }
				 }
				 if(Integer.parseInt(lineSplit[0]) == 1)
				 {
					 distance = "0";
				 }
				 else
				 {
					 distance = lineSplit[1];
				 } 
				 
				 word.set("DIST "+distance);
				 context.write( new Text(lineSplit[0]), word );
				 word.clear();
				 
				 word.set("ADJNODES "+lineSplit[3]);//tells me to append on the final tally
				 context.write( new Text(lineSplit[0]), word );
				 word.clear();
				
				
			}
		}

		/**
		 * The Class IntSumReducer.
		 */
		/*
		 * Reducer Class which contains the reduce function called on each unique key value pair 
		 * */
		public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
						
			
			/* (non-Javadoc)
			 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
			 */
			/*
			 * iterates through the individual values for each key and emits the (key, total). 
			 */
			@Override
			public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
				 String nodes = "NOTMODIFIED";
				 
				 String active = "false";
				 Counter updated =
				            context.getCounter(ActiveCounter.activeFlag);
				 Text word = new Text();
				 long lowest = 999999999;
				 
				 for (Text val : values) {

			     String[] sp = val.toString().split(" ");
				 if(sp[0].equalsIgnoreCase("ADJNODES"))
				 {
					 nodes = sp[1];
				 }
				 
				 else if(sp[0].equalsIgnoreCase("DIST"))
				 {
					 
					 long distance = Long.parseLong(sp[1]);
					 lowest = Math.min(distance, lowest);
				 
					 if(lowest< distance)
					 {
						 // System.out.println("in reducer****");
						 updated.increment(1);

					 }
				 }
				 
				 else if(sp[0].equalsIgnoreCase("FLAG"))
				 {
					 active = "true";
				//	 System.out.println("in reducer****");
				//	 System.out.println(key.toString());

				 }
				 }
				 
				 word.set(lowest+" "+active+" "+nodes);
				 context.write(key, word);
				 word.clear();
				
			}
		}
		
		/**
		 * The Enum ActiveCounter.
		 */
		static enum ActiveCounter {
	        
        	/** The active flag. */
        	activeFlag
	    }
		/*
		 * Driver code sets Mapper and Reducer class and controls program execution.
		 */

		/* (non-Javadoc)
		 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
		 */
		@Override
		public int run(final String[] args) throws Exception {
			

			
			int n = 1;
			long activeFlag = 0;
			long counterVal = 1;
			String input = args[0];
			String output = args[1];
			
			int status =0;
			
			while(activeFlag != counterVal)
			{	
				
			activeFlag = counterVal;	

			final Configuration conf = getConf();
			final Job job = Job.getInstance(conf, "Twitter Shortest Path");
			job.setJarByClass(TwitterShortestPath.class);
			
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", " ");
			
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
            status = job.waitForCompletion(true) ? 0 : 1;

			input = output;
			output = args[1] + "_" + n;
			
			Counters jobCounters = job.getCounters();
			counterVal = jobCounters.
                findCounter(ActiveCounter.activeFlag).getValue();
            n++;
			
			}
			
			return status;
			
			
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
			
				ToolRunner.run(new TwitterShortestPath(), args);
			}
			catch (final Exception e) {
			
				logger.error("", e);
			}
		}
}