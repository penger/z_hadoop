package mapreduce;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * hello	you
 * hello	me
 *
 */
public class WordCountApp {
	private static final String INPUT_PATH = "hdfs://chaoren1:9000/files";
	private static final String OUT_PATH = "hdfs://chaoren1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf , WordCountApp.class.getSimpleName());
		job.setJarByClass(WordCountApp.class);
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
			//为什么要把hadoop类型转换为java类型？
			final String line = value.toString();
			final String[] splited = line.split("\t");
			
			//产生的<k,v>对少了
			for (String word : splited) {
				//在for循环体内，临时变量word的出现次数是常量1
				context.write(new Text(word), new LongWritable(1));
			}
		};
	}
	
	//map函数执行结束后，map输出的<k,v>一共有4个，分别是<hello,1><you,1><hello,1><me,1>
	//分区，默认只有一个区
	//排序后的结果：<hello,1><hello,1><me,1><you,1>
	//分组后的结果：<hello,{1,1}>  <me,{1}>  <you,{1}>
	//归约(可选)
	
	
	
	//map产生的<k,v>分发到reduce的过程称作shuffle
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		//每一组调用一次reduce函数，一共调用了3次
		//分组的数量与reduce函数的调用次数有什么关系？
		//reduce函数的调用次数与输出的<k,v>的数量有什么关系？
		protected void reduce(Text key, java.lang.Iterable<LongWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
			//count表示单词key在整个文件中的出现次数
			long count = 0L;
			for (LongWritable times : values) {
				count += times.get();
			}
			context.write(key, new LongWritable(count));
		};
	}
}
