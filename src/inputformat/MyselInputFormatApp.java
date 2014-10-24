package inputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mapreduce.WordCountApp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 数据源来自于内存
 */
public class MyselInputFormatApp {
	private static final String OUT_PATH = "hdfs://chaoren1:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf , WordCountApp.class.getSimpleName());
		job.setJarByClass(WordCountApp.class);
		
		job.setInputFormatClass(MyselfMemoryInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<NullWritable, Text, Text, LongWritable>{
		//解析源文件会产生2个键值对，分别是<0,hello you><10,hello me>；所以map函数会被调用2次
		protected void map(NullWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<NullWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
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
	
	/**
	 * 从内存中产生数据，然后解析成一个个的键值对
	 *
	 */
	public static class MyselfMemoryInputFormat extends InputFormat<NullWritable, Text>{

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException, InterruptedException {
			final ArrayList<InputSplit> result = new ArrayList<InputSplit>();
			result.add(new MemoryInputSplit());
			result.add(new MemoryInputSplit());
			result.add(new MemoryInputSplit());
			
			return result;
		}

		@Override
		public RecordReader<NullWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new MemoryRecordReader();
		}

	}
	
	public static class MemoryInputSplit extends InputSplit implements Writable{
		final int SIZE = 10;
		final ArrayWritable arrayWritable = new ArrayWritable(Text.class);
		
		/**
		 * 先创建一个java数组类型，然后转化为hadoop的数组类型
		 */
		public MemoryInputSplit() {
			Text[] array = new Text[SIZE];
			
			final Random random = new Random();
			for (int i = 0; i < SIZE; i++) {
				final int nextInt = random.nextInt(999999);
				final Text text = new Text("Text"+nextInt);
				array[i] = text;
			}
			
			arrayWritable.set(array);
		}
		
		@Override
		public long getLength() throws IOException, InterruptedException {
			return SIZE;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] {"localhost"};
		}

		public ArrayWritable getValues() {
			return arrayWritable;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			arrayWritable.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			arrayWritable.readFields(in);
		}
		
		
	}
	
	public static class MemoryRecordReader extends RecordReader<NullWritable, Text>{
		Writable[] values = null;
		Text value = null;
		int i = 0;
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			MemoryInputSplit inputSplit = (MemoryInputSplit)split;
			ArrayWritable writables = inputSplit.getValues();
			this.values = writables.get();
			this.i = 0;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(i>=values.length) {
				return false;
			}
			if(this.value==null) {
				this.value = new Text();
			}
			this.value.set((Text)values[i]);
			i++;
			return true;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException,
				InterruptedException {
			return NullWritable.get();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			
		}
		
	}
}
