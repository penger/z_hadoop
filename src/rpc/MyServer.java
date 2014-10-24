package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {
	public static final String ADDRESS = "localhost";
	public static final int PORT = 2454;
	
	public static void main(String[] args) throws Exception{
	    /**
	     * 构造一个RPC的服务端
	     * @param instance 实例对象的方法会被客户端调用。
	     * @param bindAddress the address to bind on to listen for connection
	     * @param port the port to listen for connections on
	     * @param conf the configuration to use
	     */
		final Server server = RPC.getServer(new MyBiz(), MyServer.ADDRESS, MyServer.PORT, new Configuration());
		server.start();
	}

}