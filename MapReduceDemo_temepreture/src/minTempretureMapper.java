import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class minTempretureMapper  extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		private static final int MISSING =9999;
		
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15,19);
			
			int airTempreture ;
			if(line.charAt(87)=='+'){
				airTempreture = Integer.parseInt(line.substring(88, 92));
			}else{
				airTempreture = Integer.parseInt(line.substring(87, 92));
			}
			String quality = line.substring(92, 93);
			//正则表达式匹配数字0/1/4/5/9
			if (airTempreture != MISSING && quality.matches("[01459]")){
				context.write(new Text(year), new IntWritable(airTempreture));
			}
		}
}
