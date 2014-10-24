//package inputformat;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.net.URI;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//
//import mapreduce.WordCountApp;
//
//import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.lib.db.DBConfiguration;
//import org.apache.hadoop.mapred.lib.db.DBInputFormat;
//import org.apache.hadoop.mapred.lib.db.DBWritable;
//import org.apache.log4j.Level;
//import org.apache.log4j.LogManager;
//
///**
// * 要运行本示例
// * 1.把mysql的jdbc驱动放到各TaskTracker节点的lib目录下
// * 2.重启集群
// *
// */
//public class MyDBInputFormatApp2 {
//	private static final String OUT_PATH = "hdfs://crxy0:9000/out";
//
//	public static void main(String[] args) throws Exception{
//		LogManager.getRootLogger().setLevel(Level.toLevel(Level.DEBUG_INT));
//		
//		
//		final JobConf job = new JobConf(WordCountApp.class);
//		job.setBoolean("keep.failed.task.files", true);
//		
//		DistributedCache.addArchiveToClassPath(new Path("hdfs://crxy0:9000/tmp/mysql-connector-java-5.1.10.jar"), job);
//
//		final String s1 = job.get(DBConfiguration.DRIVER_CLASS_PROPERTY);
//		System.out.println(s1);
//		
//		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), job);
//		filesystem.delete(new Path(OUT_PATH), true);
//		
//		job.setJarByClass(WordCountApp.class);
//		
//		DBConfiguration.configureDB(job, "com.mysql.jdbc.Driver", "jdbc:mysql://crxy2:3306/test", "root", "admin");
//		
//		
//		job.setInputFormat(DBInputFormat.class);
//		DBInputFormat.setInput(job, MyUser.class, "myuser", null, null, "id", "name");
//		job.setMapperClass(MyMapper.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(NullWritable.class);
//		
//
//		job.setNumReduceTasks(0);		//指定不需要使用reduce，直接把map输出写入到HDFS中
//		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(NullWritable.class);
//		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
//		
//	
//		JobClient.runJob(job);
//	}
//	
//	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, MyUser, Text, NullWritable>{
//
//		@Override
//		public void map(LongWritable key, MyUser value,
//				OutputCollector<Text, NullWritable> collector, Reporter reporter)
//				throws IOException {
//			collector.collect(new Text(value.toString()), NullWritable.get());
//		};
//	}
//
//	
//	public static class MyUser implements Writable, DBWritable{
//		int id;
//		String name;
//		
//		@Override
//		public void write(DataOutput out) throws IOException {
//			out.writeInt(id);
//			Text.writeString(out, name);
//		}
//
//		@Override
//		public void readFields(DataInput in) throws IOException {
//			this.id = in.readInt();
//			this.name = Text.readString(in);
//		}
//
//		@Override
//		public void write(PreparedStatement statement) throws SQLException {
//			statement.setInt(1, id);
//			statement.setString(2, name);
//		}
//
//		@Override
//		public void readFields(ResultSet resultSet) throws SQLException {
//			this.id = resultSet.getInt(1);
//			this.name = resultSet.getString(2);
//		}
//
//		@Override
//		public String toString() {
//			return id + "\t" + name;
//		}
//		
//	}
//}
