package assignment_6_1_medal_count_olympics;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class medalCountDriver
{

	public static void main (String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(medalCountDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(numMedalsEachCountryWonInSwimmingMap.class);
		job.setCombinerClass(medalCountReduce.class);
		job.setReducerClass(medalCountReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Start Execution by submitting the job
		
		job.waitForCompletion(true);
		//System.exit( job_status ? 0 : 1);
		
		// Job for submitting mapper/reducer code for counting medals for India year wise
		@SuppressWarnings("deprecation")
		Job job2 = new Job();
		job2.setJarByClass(medalCountDriver.class);
					
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			
		job2.setMapperClass(numMedalsIndiaWonYearwiseMap.class);
		job2.setCombinerClass(medalCountReduce.class);
		job2.setReducerClass(medalCountReduce.class);
				
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
					
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
			
		//Start Execution by submitting the job
		boolean job2_status = job2.waitForCompletion(true);
		System.exit( job2_status ? 0 : 1);
				
	}
}
