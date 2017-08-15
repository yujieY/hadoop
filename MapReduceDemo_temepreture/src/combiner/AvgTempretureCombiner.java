package combiner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTempretureCombiner extends Reducer<Text, Text, Text, Text>{
	
		public void reduce(Text key ,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			double sumvalue = 0;
			long numvalue = 0;
			
			for(Text values :value){
				sumvalue += Double.parseDouble(values.toString());
				numvalue ++;
			}
			context.write(key, new Text(String.valueOf(sumvalue)+","+String.valueOf(numvalue)));
		}
}
