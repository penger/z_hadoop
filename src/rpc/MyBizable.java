package rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBizable extends VersionedProtocol{
	public static final int PORT = 12345;
	public abstract String hello(String name);

	public abstract long getProtocolVersion(String protocol, long clientVersion)
			throws IOException;

}