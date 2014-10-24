package mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KpiApp2 {

	public static final String INPUT_PATH = "hdfs://chaoren1:9000/kpi";
	public static final String OUT_PATH = "hdfs://chaoren1:9000/kpi_out";

	public static void main(String[] args) throws Exception{
		final Configuration conf = new Configuration();
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf, KpiApp2.class.getSimpleName());
		job.setJarByClass(KpiApp2.class);
		
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		
		job.setMapperClass(MyMapper.class);
		//当reduce输出类型与map输出类型一致时，map输出类型可以不设置
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KpiWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}

	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {
			final String line = value.toString();
			final String[] splited = line.split("\t");
			final String mobileNumber = splited[1];
			final Text k2 = new Text(mobileNumber);
			final KpiWritable v2 = new KpiWritable(Long.parseLong(splited[6]), Long.parseLong(splited[7]), Long.parseLong(splited[8]), Long.parseLong(splited[9]));
			context.write(k2, v2);
		};
	}
	
	public static class MyReducer extends Reducer<Text, KpiWritable, Text, NullWritable>{
		protected void reduce(Text k2, java.lang.Iterable<KpiWritable> v2s, org.apache.hadoop.mapreduce.Reducer<Text,KpiWritable,Text,NullWritable>.Context context) throws IOException ,InterruptedException {
			long upPackNum	=0L;
			long downPackNum=0L;
			long upPayLoad	=0L;
			long downPayLoad=0L;
			
			for (KpiWritable kpiWritable : v2s) {
				upPackNum += kpiWritable.upPackNum;
				downPackNum += kpiWritable.downPackNum;
				upPayLoad += kpiWritable.upPayLoad;
				downPayLoad += kpiWritable.downPayLoad;
			}
			
			
			context.write(new Text(k2.toString() + "\t" + upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad), NullWritable.get());
		};
	}
}