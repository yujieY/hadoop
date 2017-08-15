import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class minTepreture {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Job job = new Job();
			job.setJarByClass(minTepreture.class);
			job.setJobName("minTempreture");
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(minTempretureMapper.class);
			job.setReducerClass(minTempretureReduceer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.exit(job.waitForCompletion(true)? 0 : 1);	
	}

}
