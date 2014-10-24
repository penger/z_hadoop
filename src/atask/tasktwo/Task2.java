package atask.tasktwo;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import atask.taskone.Task1;

/**
 * 自定义OutputFormat，实现单词统计，把输出记录写入到2个文件中，文件名分别是a和b
 * @author Esc_penger
 *
 */
public class Task2 {

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
		jobConf.setOutputFormat(MyOutputFormat.class);
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
	
	public static class MyOutputFormat <K, V> extends FileOutputFormat<K, V>{
		
		protected static class MyRecordWriter <K,V>implements RecordWriter<K, V>{
			
			private static final String utf8="UTF-8";
			//bytes
			private static byte[] newline;
			
			protected DataOutputStream out;
			private static byte[] keyValueSeparator;
			
			static{
					try {
						//设置分隔符为--------
						keyValueSeparator="----------".getBytes(utf8);
						newline="/n".getBytes(utf8);
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
			}



			public MyRecordWriter(DataOutputStream out) {
				super();
				this.out = out;
			}

			@Override
			public synchronized void write(K key, V value) throws IOException {
				  boolean nullKey = key == null || key instanceof NullWritable;
			      boolean nullValue = value == null || value instanceof NullWritable;
			      if (nullKey && nullValue) {
			        return;
			      }
			      if (!nullKey) {
			        writeObject(key);
			      }
			      if (!(nullKey || nullValue)) {
			        out.write(keyValueSeparator);
			      }
			      if (!nullValue) {
			        writeObject(value);
			      }
			      out.write(newline);
			      out.write(newline);
			}

		    private void writeObject(Object o) throws IOException {
		        if (o instanceof Text) {
		          Text to = (Text) o;
		          out.write(to.getBytes(), 0, to.getLength());
		        } else {
		          out.write(o.toString().getBytes(utf8));
		        }
		      }

			@Override
			public synchronized void close(Reporter reporter) throws IOException {
				out.close();
				
			}
			
		}

		@Override
		public RecordWriter<K, V> getRecordWriter(FileSystem ignored,
				JobConf job, String name, Progressable progress)
				throws IOException {
			boolean isCompressed = getCompressOutput(job);
			String keyValueSeparator = job.get("mapred.textoutputformat.separator", 
                    "\t");
			//是不是压缩结果
			if(!isCompressed){
				name="abc";
				Path file = FileOutputFormat.getTaskOutputPath(job, name);
				FileSystem fileSystem = file.getFileSystem(job);
				FSDataOutputStream fileOut = fileSystem.create(file, progress);
				return new MyRecordWriter<K, V>(fileOut);
			}else{
				Class<? extends CompressionCodec> codeClass = getOutputCompressorClass(job, GzipCodec.class);
				CompressionCodec codec = ReflectionUtils.newInstance(codeClass, job);
				Path file = FileOutputFormat.getTaskOutputPath(job, name+codec.getDefaultExtension());
				FileSystem fileSystem = file.getFileSystem(job);
				FSDataOutputStream fileOut = fileSystem.create(file,progress);
				DataOutputStream outputStream = new DataOutputStream(codec.createOutputStream(fileOut));
				return new MyRecordWriter<K, V>(outputStream);
			}
		}
		
	
	
	

		
	}
	
	
}
