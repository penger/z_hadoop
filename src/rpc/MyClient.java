package rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {
	public static void main(String[] args) throws Exception{
		MyBizable client = (MyBizable) RPC.getProxy(MyBizable.class, MyBizable.PORT, new InetSocketAddress(MyServer.ADDRESS, MyServer.PORT), new Configuration());
		final String result = client.hello("world");
		System.out.println(result);
	}
}
