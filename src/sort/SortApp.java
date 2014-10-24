package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortApp {
	private static final String INPUT_PATH = "hdfs://chaoren1:9000/data";
	private static final String OUT_PATH = "hdfs://chaoren1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf , SortApp.class.getSimpleName());
		job.setJarByClass(SortApp.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(NewK2.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, NewK2, LongWritable>{
		//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,NewK2,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
			final String line = value.toString();
			final String[] splited = line.split("\t");
			
			context.write(new NewK2(Long.parseLong(splited[0]), Long.parseLong(splited[1])), new LongWritable(Long.parseLong(splited[1])));
		};
	}
	
	
	public static class MyReducer extends Reducer<NewK2, LongWritable, LongWritable, LongWritable>{
		protected void reduce(NewK2 key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<NewK2,LongWritable,LongWritable,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
			context.write(new LongWritable(key.first), new LongWritable(key.second));
		};
	}
	
	public static class NewK2 implements WritableComparable<NewK2>{
		long first;
		long second;
		
		public NewK2() {
		}
		
		public NewK2(long first, long second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.first);
			out.writeLong(this.second);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.first = in.readLong();
			this.second = in.readLong();
		}

		@Override
		public int compareTo(NewK2 o) {
			final long minus = this.first - o.first;
			if(minus!=0) {
				return (int)minus;
			}
			return (int)(this.second - o.second);
		}
		
	}
}
