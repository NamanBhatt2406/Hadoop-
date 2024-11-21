import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class missingGenre {
	public static class Map extends Mapper<LongWritable, Text, Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
			 String[] col = value.toString().split(",");
			 
			if (col.length < 3 || col[2].length() == 0) {
                context.write( new Text("No. of Missing Genre are = "), new IntWritable(1));
            }
			
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable, Text, IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException,InterruptedException {
			int count = 0;
			for(IntWritable i:value) {
				count+= i.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"wordCount");
		
		job.setJarByClass(missingGenre.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
