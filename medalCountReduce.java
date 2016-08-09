package assignment_6_1_medal_count_olympics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class medalCountReduce extends Reducer
			<Text, IntWritable, Text,IntWritable>
{
	//@Overiding abstract method reduce() from Reducer class
	public void reduce(Text keyIn, Iterable <IntWritable> valueIn, Context con)
				throws IOException, InterruptedException
	{
		int sum = 0;
		
		for (IntWritable val : valueIn)
		{
			sum += val.get();
		}
		
		con.write(keyIn, new IntWritable(sum));
	}
}
