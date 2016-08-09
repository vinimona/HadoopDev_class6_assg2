package assignment_6_1_medal_count_olympics;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class numMedalsEachCountryWonInSwimmingMap extends Mapper <LongWritable, Text, Text,IntWritable> 
{
	//@Overiding abstract method map() from Mapper class
	public void map(LongWritable keyIn, Text valueIn, Context con) 
						throws IOException, InterruptedException
	{
		StringTokenizer st = new StringTokenizer(valueIn.toString().replaceAll(",,", ",NULL,"), ",");
		int totalTokens = st.countTokens();
		String tokens[] = new String[totalTokens];
		boolean swimming = false;
		String country = new String();
		int total_medals = 0;
 		for (int i = 0; i < totalTokens; i++)
		{
			tokens[i] = st.nextToken();
			if (i == 5 && tokens[i].toUpperCase().equalsIgnoreCase("Swimming"))
				swimming = true;
			if (i == 2)
				country = tokens[i];
			if (i == 9)
				total_medals = Integer.parseInt(tokens[i]);
			if (st.countTokens() == 0)
				break;
		}
 		
 		if (swimming)
 		{
 			con.write(new Text(country), new IntWritable(total_medals));
 		}
	}
}



// tokens[2] -> Country
// tokens[5] -> sport
// tokens[9] -> total medals

