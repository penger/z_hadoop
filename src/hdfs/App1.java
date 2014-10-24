package hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class App1 {

	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		
		final URL url = new URL("hdfs://chaoren1:9000/hello2");
		final InputStream is = url.openStream();
		  /**
		   * Copies from one stream to another.
		   * @param in 输入流
		   * @param out 输出流
		   * @param bufferSize 换成区大小 
		   * @param close 是否关闭流
		   */
		IOUtils.copyBytes(is, System.out, 1024, true);
	}

}
