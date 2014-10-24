package inputformat;

import java.net.URI;

import mapreduce.WordCountApp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyGenericWritableApp {
	private static final String INPUT_PATH = "hdfs://chaoren1:9000/files";
	private static final String OUT_PATH = "hdfs://chaoren1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf , WordCountApp.class.getSimpleName());
		job.setJarByClass(WordCountApp.class);
		
		MultipleInputs.addInputPath(job, new Path("hdfs://chaoren1:9000/files/hello"), KeyValueTextInputFormat.class, MyMapper.class);
		MultipleInputs.addInputPath(job, new Path("hdfs://chaoren1:9000/files/hello2"), TextInputFormat.class, MyMapper2.class);
		
		
		//job.setMapperClass(MyMapper.class);	//不应该有这一行
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MyGenericWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<Text, Text, Text, MyGenericWritable>{
		//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次
		protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper<Text,Text,Text,MyGenericWritable>.Context context) throws java.io.IOException ,InterruptedException {
				context.write(key, new MyGenericWritable(new LongWritable(1)));
				context.write(value, new MyGenericWritable(new LongWritable(1)));
		};
	}
	
	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, MyGenericWritable>{
		//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,MyGenericWritable>.Context context) throws java.io.IOException ,InterruptedException {
			//为什么要把hadoop类型转换为java类型？
			final String line = value.toString();
			final String[] splited = line.split(",");
			
			//产生的<k,v>对少了
			for (String word : splited) {
				//在for循环体内，临时变量word的出现次数是常量1
				final Text text = new Text("1");
				context.write(new Text(word), new MyGenericWritable(text));
			}
		};
	}
	
	//map产生的<k,v>分发到reduce的过程称作shuffle
	public static class MyReducer extends Reducer<Text, MyGenericWritable, Text, LongWritable>{
		//每一组调用一次reduce函数，一共调用了3次
		//分组的数量与reduce函数的调用次数有什么关系？
		//reduce函数的调用次数与输出的<k,v>的数量有什么关系？
		protected void reduce(Text key, java.lang.Iterable<MyGenericWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,MyGenericWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
			//count表示单词key在整个文件中的出现次数
			long count = 0L;
			for (MyGenericWritable times : values) {
				final Writable writable = times.get();
				if(writable instanceof LongWritable) {
					count += ((LongWritable)writable).get();					
				}
				if(writable instanceof Text) {
					count += Long.parseLong(((Text)writable).toString());
				}
			}
			context.write(key, new LongWritable(count));
		};
	}
	
	public static class MyGenericWritable extends GenericWritable{
		public MyGenericWritable() {}
		
		public MyGenericWritable(Text text) {
			super.set(text);
		}

		public MyGenericWritable(LongWritable longWritable) {
			super.set(longWritable);
		}

		@Override
		protected Class<? extends Writable>[] getTypes() {
			return new Class[] {LongWritable.class, Text.class};
		}
		
	}
}
