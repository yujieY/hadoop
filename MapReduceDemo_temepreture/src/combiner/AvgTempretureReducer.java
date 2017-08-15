package combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTempretureReducer extends Reducer<Text, Text, Text, IntWritable>{

		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			double sumvalue = 0;
			long numvalue = 0;
			int avgvalue = 0;
			
			for (Text values : value){
				String[] valueAll = values.toString().split(",");
				sumvalue += Double.parseDouble(valueAll[0]);
				numvalue += Long.parseLong(valueAll[1]);
			}
			avgvalue = (int) (sumvalue/numvalue);
			context.write(key, new IntWritable(avgvalue));
		}
}
