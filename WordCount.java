package Hadoop;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool{

	public static class WordMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
		
		private Text word = new Text();
		private IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { ;
				String[] tokens = value.toString().split("[\\W]");
				
				for (String token : tokens) {
					word.set(token);
					context.write(word,one);
				}
			
			
		}
		
		
	}
	
	public static class WordReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0 ;
			for (IntWritable value : values){
				sum = sum + value.get() ;
			}
			context.write(key, new IntWritable(sum));
			
		}
		
	}
	public static void main(String [] args) throws Exception {
		
		Configuration conf = new Configuration();
		ToolRunner.run(conf,new WordCount(), args);
		//ToolRunner runner = new ToolRunner();
	}
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Word Count");
		
		job.setJarByClass(WordCount.class);
		
		
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		
		return job.waitForCompletion(true)?0:1;
	}
}
