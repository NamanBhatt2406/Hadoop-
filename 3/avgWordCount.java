import java.io.IOException;
import java.util.StringTokenizer;

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

public class avgWordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String line=value.toString();
			StringTokenizer token=new StringTokenizer(line);
			while(token.hasMoreElements())
			{
				value.set(token.nextToken());
				context.write(value, new IntWritable(1));
			}
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private int totalWords = 0;
        private int totalOccurrences = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalWords++;
            totalOccurrences += sum;
            context.write(key, new Text(String.valueOf(sum)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double average = (double) totalOccurrences / totalWords;
            context.write(new Text("AverageCount"), new Text(String.valueOf(average)));
        }
    }
	
	public static void main(String[] args)throws Exception
	{
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
		
		//job.setJobName("CountCounters");
		//job.setJar("CountCounters.jar");
		job.setJarByClass(avgWordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class); 
		job.setNumReduceTasks(1);
		 
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}