package wc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class KMeans_Impl.
 */
public class KMeans_Impl extends Configured implements Tool {
	
	/** The centroid array. */
	public static List<Long> centroidArray = new ArrayList<>();

	
		/** The Constant logger. */
		private static final Logger logger = LogManager.getLogger(KMeans_Impl.class);

		/**
		 * Mapper Class which contains the map function called on each input record
		 * *.
		 */
		public static class TokenizerMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		
			
			/**
			 * Sets the up.
			 *
			 * @param context the new up
			 * @throws IOException Signals that an I/O exception has occurred.
			 */
			@Override
		public void setup(Context context) throws IOException
		{				
			try
			{
					 centroidArray.clear();
	                    
					 String line;

		             URI[] cacheFiles = context.getCacheFiles();
		                
		             for(int i=0; i<cacheFiles.length; i++)
		               {
		                    
		            	 URI cacheFile = cacheFiles[i];
		                  
		            	 FileSystem fs = FileSystem.get(cacheFile, new Configuration());
		            	 InputStreamReader inputStream = new InputStreamReader(fs.open(new Path(cacheFile.getPath())));
		                    BufferedReader reader = new BufferedReader(inputStream);
		                    try
		                    {
		                        while ((line = reader.readLine()) != null) 
		                        {
		                        	centroidArray.add(Long.parseLong(line));
		                        }
		                    }
		                    
		                    finally 
		                    {
		                    	reader.close();
		                    }
		                }
		            } catch (IOException e) 
				 {
		            }
			}
			
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
			public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
				// Split the line of input record based on delimiter ","
				
				 String line = value.toString();
				 
				 String[] lineSplit = line.split(",");
				 long point = Long.parseLong(lineSplit[1]);
				 
				 long closestCentre = centroidArray.get(0);
				 Long minDistance = Math.abs(point - closestCentre);
				 
				 for(int i =0;i<centroidArray.size();i++)
				 {
					 if( Math.abs(point - centroidArray.get(i)) < minDistance)
					 {
						 closestCentre = centroidArray.get(i);
						 minDistance = Math.abs(point - centroidArray.get(i));
						 
					 }
				 }
				 
				context.write(new LongWritable(closestCentre)
						,new LongWritable(point)); 
				
			}
		}
		
		/**
		 * The Enum ActiveCounter.
		 */
		static enum ActiveCounter {
	        
        	/** The active flag. */
        	activeFlag
}

		/**
		 * The Class IntSumReducer.
		 */
		/*
		 * Reducer Class which contains the reduce function called on each unique key value pair 
		 * */
		public static class IntSumReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
						
			/** The text. */
			public static Text text = new Text();
			
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
			public void reduce(final LongWritable key, final Iterable<LongWritable> values, final Context context) throws IOException, InterruptedException {
				
				long newCentroid;
				long sum = 0;
				int noOfElements = 0;
				long co_ordinate;
				String follPoints = null;
				long sse = 0;
				List<Long> valArray = new ArrayList<>();
				
				for (final LongWritable val : values)
				{
					co_ordinate = val.get();
					sum = sum + co_ordinate;
					++noOfElements;
					follPoints = val.toString();
					valArray.add(co_ordinate);
				}
				

				// We have new center now
				newCentroid = sum / noOfElements;
				
				 Counter updated =
						 context.getCounter(ActiveCounter.activeFlag);
				 
				 for(final long val : valArray)
				 {
					 sse += Math.pow((newCentroid - val), 2);
				 }
				 
				updated.increment(sse);
				System.out.println("sum of squared error "+sse);
				context.write(new LongWritable(newCentroid),null);
				
				
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
			
			int n = 1;
			long activeFlag = 0;
			long counterVal = 1;
			String input = args[0];
			String output = args[1];
			String centroids = args[2];
			int no_of_iter = 0;
			
			int status =0;
			
			while(activeFlag != counterVal && no_of_iter<10)
			{ 
			activeFlag = counterVal;	

			final Configuration conf = getConf();
			final Job job = Job.getInstance(conf, "Kmeans");
			job.setJarByClass(KMeans_Impl.class);
			
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", " ");
			
			Path path = new Path(centroids);
	        FileSystem fs = FileSystem.get(new URI(centroids), new Configuration());
	        FileStatus[] fileStat = fs.listStatus(path);
	        for(FileStatus f : fileStat) {
	            job.addCacheFile(f.getPath().toUri());
	        }
	        
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
            status = job.waitForCompletion(true) ? 0 : 1;

			centroids = output;
			output = args[1] + "_" + n;
			
			Counters jobCounters = job.getCounters();
			counterVal = jobCounters.
                findCounter(ActiveCounter.activeFlag).getValue();
			System.out.println(counterVal);
            n++;
			no_of_iter++;
			}
			
			
			System.out.println("no of iters "+no_of_iter);
			return status;
			
			
		}

		/**
		 * The main method.
		 *
		 * @param args the arguments
		 */
		public static void main(final String[] args) {
			
		
			if (args.length != 3) {
				throw new Error("Three arguments required:\n<input-dir> <output-dir>");
			}

			try {
			
				ToolRunner.run(new KMeans_Impl(), args);
			}
			catch (final Exception e) {
			
				logger.error("", e);
			}
		}
}

