package hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class App2 {
	public static void main(String[] args) throws Exception{
		final FileSystem fileSystem = FileSystem.get(new URI("hdfs://chaoren1:9000"), new Configuration());
		System.out.println(fileSystem.getClass().getName());
		
		
		//创建文件夹
		mkdir(fileSystem);
		//上传
		putdata(fileSystem);
		//下载
		download(fileSystem);
		//查看目录列表
		final FileStatus[] listStatuses = fileSystem.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatuses) {
			System.out.println(fileStatus.getPath().toString());
		}
		System.out.println("-------------------------");
		//删除
		final boolean isDeleted = fileSystem.delete(new Path("/dir1"), true);
		if(isDeleted) {
			System.out.println("删除成功");
		}
	}

	private static void download(final FileSystem fileSystem)
			throws IOException {
		final FSDataInputStream in = fileSystem.open(new Path("/dir1/readme"));
		IOUtils.copyBytes(in, System.out, 1024, false);
		in.close();
		System.out.println("-------------------------");
	}

	private static void putdata(final FileSystem fileSystem)
			throws IOException, FileNotFoundException {
		final FSDataOutputStream out = fileSystem.create(new Path("/dir1/readme"));
		final FileInputStream in = new FileInputStream("/mnt/home/cr00/data/hello");
		IOUtils.copyBytes(in, out, 1024, true);
	}

	private static void mkdir(final FileSystem fileSystem) throws IOException {
		final boolean successful = fileSystem.mkdirs(new Path("/dir1"));
		if(successful) {
			System.out.println("创建目录成功");
		}
	}
}
