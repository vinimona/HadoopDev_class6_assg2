package assignment_6_1_medal_count_olympics;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class numMedalsIndiaWonYearwiseMap extends Mapper <LongWritable, Text, Text,IntWritable>
{
		//@Overiding abstract method map() from Mapper class
		public void map(LongWritable keyIn, Text valueIn, Context con) 
							throws IOException, InterruptedException
		{
			StringTokenizer st = new StringTokenizer(valueIn.toString().replaceAll(",,", ",NULL,"), ",");
			int totalTokens = st.countTokens();
			String tokens[] = new String[totalTokens];
			String country = new String();
			String year = new String();
			int total_medals = 0;
	 		for (int i = 0; i < totalTokens; i++)
			{
				tokens[i] = st.nextToken();
				if (i == 3)
					year = tokens[i];
				if (i == 2)
					country = tokens[i];
				if (i == 9)
					total_medals = Integer.parseInt(tokens[i]);
				if (st.countTokens() == 0)
					break;
			}
	 		
	 		if (country.toUpperCase().equalsIgnoreCase("India"))
	 		{
	 			con.write(new Text(year), new IntWritable(total_medals));
	 		}
		}
}
