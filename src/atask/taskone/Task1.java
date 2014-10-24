package atask.taskone;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
/**
 * 自定义InputFormat ,从linux本地文件/etc/profile读取数据然后进行数据统计
 * @author Esc_penger
 *
 */
public class Task1 {
	
	public static void main(String[] args) throws Exception{
		final FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.114.137:9000"), new Configuration());
		//1.将数据从linux本地磁盘上传到hdfs中
		String localFilename="/etc/profile";
		String hdfsFilename="/input/profile";
		String hdfsFilenameOutput="/output";
		
		cleanhdfsworkspace(fileSystem, hdfsFilename, hdfsFilenameOutput);
//		copyLocalFile2hdfs(localFilename,hdfsFilename,fileSystem);
		//2.自定义mapper,reducer
		//3.执行方法
		JobConf jobConf = new JobConf(Task1.class);
		jobConf.setMapperClass(CountMapper.class);
		jobConf.setReducerClass(CountReducer.class);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(LongWritable.class);
		jobConf.setJobName("name");
		FileInputFormat.setInputPaths(jobConf, new Path(hdfsFilename));
		FileOutputFormat.setOutputPath(jobConf, new Path(hdfsFilenameOutput));
		JobClient.runJob(jobConf);
	}
	
	private static void cleanhdfsworkspace(FileSystem fileSystem, String inputpath,String outputpath) throws Exception{
		//输入路径保证有文件
		boolean checkFileExist = checkFileExist(inputpath, fileSystem);
		if(!checkFileExist){
			throw new Exception();
		}
		//输出路径保证为空
		if(checkFileExist(outputpath,fileSystem)){
			fileSystem.deleteOnExit(new Path(outputpath));
		}
	}
	
	
	private static boolean checkFileExist(String hdfsfilename,FileSystem fileSystem) throws IOException{
		return fileSystem.exists(new Path(hdfsfilename));
	}

	private static void copyLocalFile2hdfs(String localFilename,
			String hdfsFilename,FileSystem fileSystem) throws IOException {
		fileSystem.copyFromLocalFile(new Path(localFilename), new Path(hdfsFilename));
	}

	public static  class  CountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
			String next="";
			while(stringTokenizer.hasMoreTokens()){
				next = stringTokenizer.nextToken();
			}
			collector.collect(new Text(next), new LongWritable(1l));
		}
		
	}
	
	public static class CountReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			LongWritable countvalue=null;
			long sum=0l;
			while(values.hasNext()){
				LongWritable next = values.next();
				sum+=next.get();
				countvalue=new LongWritable(sum);
			}
			collector.collect(key, countvalue);	
		}

	}
}
