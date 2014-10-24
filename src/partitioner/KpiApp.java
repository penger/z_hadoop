package partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class KpiApp {

	public static final String INPUT_PATH = "hdfs://chaoren1:9000/kpi";
	public static final String OUT_PATH = "hdfs://chaoren1:9000/kpi_out";

	public static void main(String[] args) throws Exception{
		final Configuration conf = new Configuration();
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf, KpiApp.class.getSimpleName());
		job.setJarByClass(KpiApp.class);
		
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);

		job.setNumReduceTasks(2);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}

	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, KpiWritable>{
		final Text k2 = new Text();
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {
			final String line = value.toString();
			final String[] splited = line.split("\t");
			final String mobileNumber = splited[1];
			k2.set(mobileNumber);
			final KpiWritable v2 = new KpiWritable(Long.parseLong(splited[6]), Long.parseLong(splited[7]), Long.parseLong(splited[8]), Long.parseLong(splited[9]));
			context.write(k2, v2);
		};
	}
	
	public static class MyReducer extends Reducer<Text, KpiWritable, Text, KpiWritable>{
		final KpiWritable v3 = new KpiWritable();
		protected void reduce(Text k2, java.lang.Iterable<KpiWritable> v2s, org.apache.hadoop.mapreduce.Reducer<Text,KpiWritable,Text,KpiWritable>.Context context) throws IOException ,InterruptedException {
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
			
			v3.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
			context.write(k2, v3);
		};
	}
	
	
	static class MyPartitioner extends Partitioner<Text, KpiWritable>{

		@Override
		public int getPartition(Text key, KpiWritable value, int numPartitions) {
			final int length = key.toString().length();
			return length==11?0:1;
		}
		
	}
}

class KpiWritable  implements Writable{
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;
	
	
	public KpiWritable() {}
	
	public KpiWritable(long upPackNum, long downPackNum, long upPayLoad,
			long downPayLoad) {
		super();
		set(upPackNum, downPackNum, upPayLoad, downPayLoad);
	}
	
	public void set(long upPackNum, long downPackNum, long upPayLoad,
			long downPayLoad) {
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.upPackNum);
		out.writeLong(this.downPackNum);
		out.writeLong(this.upPayLoad);
		out.writeLong(this.downPayLoad);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readLong();
		this.downPackNum = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
	}

	@Override
	public String toString() {
		return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;
	}
	
	
}
