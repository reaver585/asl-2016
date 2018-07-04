package ch.ethz.asltest;

import java.nio.channels.SocketChannel;

// this helper class holds a message in a data structure
// together with memaslap client's socket and reference to our
// mwServer
class Message {
	public MiddlewareServer server;
	public SocketChannel socket;
	public byte[] data;

	// 4 instrumentation parameters are held in instances of this class
	public boolean isGet = true;
	public boolean isLogged = false;
	public long responseTime;
	public long queueTime;
	public long processingTime;
	public boolean successFlag = true;

	public static final String set = ",set";
	public static final String get = ",get";

	public Message(MiddlewareServer server, SocketChannel socket, byte[] data) {
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
}
