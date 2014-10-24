package rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public class MyBiz implements  MyBizable{
	/* (non-Javadoc)
	 * @see rpc.MyBizable#hello(java.lang.String)
	 */
	@Override
	public String hello(String name) {
		System.out.println("我被调用了");
		return "hello "+name;
	}

	/* (non-Javadoc)
	 * @see rpc.MyBizable#getProtocolVersion(java.lang.String, long)
	 */
	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return MyBizable.PORT;
	}
}
