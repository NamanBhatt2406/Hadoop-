import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class minTemp {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
			String col[] = value.toString().split(",");
			String year = col[0];
			String q1 = col[1];
			String q2 = col[2];
			String q3 = col[3];
			String q4 = col[4];
			if(Integer.parseInt(q1) <= Integer.parseInt(q2) && Integer.parseInt(q1) <= Integer.parseInt(q3) && Integer.parseInt(q1) <= Integer.parseInt(q4)) {
				context.write(new Text(year), new Text(q1));
			}else if(Integer.parseInt(q2) <= Integer.parseInt(q3) && Integer.parseInt(q2) <= Integer.parseInt(q4)) {
				context.write(new Text(year), new Text(q2));
			}else if(Integer.parseInt(q3) <= Integer.parseInt(q4)) {
				context.write(new Text(year), new Text(q3));
			}else {
				context.write(new Text(year), new Text(q4));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException {
			
			for(Text detail:value)
			{
				context.write(key, detail);								
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Avarage");
		
		job.setJarByClass(minTemp.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true)? 0:1);
		
	}
}
