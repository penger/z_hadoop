package inputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import mapreduce.WordCountApp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 要运行本示例
 * 1.把mysql的jdbc驱动放到各TaskTracker节点的lib目录下
 * 2.重启集群
 *
 */
public class MyDBInputFormatApp {
	private static final String OUT_PATH = "hdfs://hadoop0:9000/out";

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://hadoop0:3306/test", "root", "admin");
		
		final FileSystem filesystem = FileSystem.get(new URI(OUT_PATH), conf);
		filesystem.delete(new Path(OUT_PATH), true);
		
		final Job job = new Job(conf , WordCountApp.class.getSimpleName());
		job.setJarByClass(WordCountApp.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		DBInputFormat.setInput(job, MyUser.class, "myuser", null, null, "id", "name");
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		

		job.setNumReduceTasks(0);		//指定不需要使用reduce，直接把map输出写入到HDFS中
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		
		job.waitForCompletion(true);
	}
	
	public static class MyMapper extends Mapper<LongWritable, MyUser, Text, NullWritable>{
		protected void map(LongWritable key, MyUser value, org.apache.hadoop.mapreduce.Mapper<LongWritable,MyUser,Text,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {
			context.write(new Text(value.toString()), NullWritable.get());
		};
	}

	
	public static class MyUser implements Writable, DBWritable{
		int id;
		String name;
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(id);
			Text.writeString(out, name);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.name = Text.readString(in);
		}

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setInt(1, id);
			statement.setString(2, name);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.id = resultSet.getInt(1);
			this.name = resultSet.getString(2);
		}

		@Override
		public String toString() {
			return id + "\t" + name;
		}
		
	}
}
