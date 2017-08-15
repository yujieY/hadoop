import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class minTempretureReduceer extends Reducer<Text, IntWritable, Text, IntWritable>{

		public void reduce(Text key ,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int minValue = Integer.MAX_VALUE;
			for(IntWritable values : value){
				minValue = Math.min(minValue, values.get());
			}
			context.write(key, new IntWritable(minValue));
		}
}
