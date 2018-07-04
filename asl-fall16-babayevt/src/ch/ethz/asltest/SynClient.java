package ch.ethz.asltest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;

public class SynClient implements Runnable {

	private String hostAddress;

	private int port;

	private ByteBuffer readBuffer = ByteBuffer.allocate(2048);

	private ByteBuffer writeBuffer = ByteBuffer.allocate(1024);

	// main queue passed by messageProcessor
	private BlockingQueue<Message> queue;

	// we gonna have only 1 socket channel
	private SocketChannel socketChannel;

	public SynClient(String hostAddress, int port, BlockingQueue<Message> queue)
			throws IOException {
		this.hostAddress = hostAddress;
		this.port = port;
		this.queue = queue;
		this.socketChannel = SocketChannel.open();
		// put channel in blocking mode for synchronous requests handling
		this.socketChannel.configureBlocking(true);
		this.socketChannel.connect(new InetSocketAddress(InetAddress
				.getByName(this.hostAddress), this.port));
	}

	public void run() {
		while (true) {
			try {
				// use thread safe .take() method since other instances
				// of this class will try to access same queue 
				Message currentRequest = queue.take();
				// finalize logging of queue time for request
				if (currentRequest.isLogged) {
					currentRequest.queueTime = (System.nanoTime() - currentRequest.queueTime) / 1000;
				}
				try {
					// first write, then read
					// read is blocking a thread until there is some response
					this.write(currentRequest);
					this.read(currentRequest);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void write(Message request) throws IOException {
		this.writeBuffer.clear();
		this.writeBuffer = ByteBuffer.wrap(request.data);
		this.socketChannel.write(this.writeBuffer);
		// start logging processing time
		if (request.isLogged) {
			request.processingTime = System.nanoTime();
		}
	}

	private void read(Message request) throws IOException {
		this.readBuffer.clear();
		int numRead = this.socketChannel.read(this.readBuffer);
		// finalize logging of processing time
		if (request.isLogged) { 
			request.processingTime = (System.nanoTime() - request.processingTime) / 1000;
		}
		byte[] dataCopy = new byte[numRead];
		System.arraycopy(this.readBuffer.array(), 0, dataCopy, 0, numRead);
		// if first letter of response is "E" it means we received
		// an "END" response, despite ideally response should start
		// with word "VALUE"
		// and then we set unsuccessful flag for this request
		if (dataCopy[0] == 69) {
			request.successFlag = false;
		}
		// send response back to memaslap client
		request.server.send(request.socket, dataCopy);
	}
}