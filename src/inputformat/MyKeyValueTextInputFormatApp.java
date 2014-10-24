package inputformat;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * hello	you	hello	you
 * hello	me
 *
 */
public class MyKeyValueTextInputFormatApp {
	private static final String INPUT_PATH = "hdfs://chaoren1:9000/hello";
	private static final String OUT_PATH = "hdfs://chaoren1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
		
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf , MyKeyValueTextInputFormatApp.class.getSimpleName());
		job.setJarByClass(MyKeyValueTextInputFormatApp.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable>{
		protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<Text,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
				context.write(key, new LongWritable(1));
				context.write(value, new LongWritable(1));
		};
	}
}
