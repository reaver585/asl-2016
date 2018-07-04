package ch.ethz.asltest;

import java.nio.channels.SocketChannel;

// this class is just a data structure
// we put instances of this class to queueToMemaslap
// in our MiddlewareServer to queue changes of keys
public class ChangeKey {

	public SocketChannel socket;
	public int ops;

	public ChangeKey(SocketChannel socket, int ops) {
		this.socket = socket;
		this.ops = ops;
	}
}