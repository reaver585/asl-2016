package ch.ethz.asltest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

public class AsynClient implements Runnable {

	// id of primary server for this async client
	private int id;

	private List<String> addresses = new ArrayList<String>();

	private int[] ports;

	// this field will also hold reference to mwServer
	// we need it to send response back to memaslap
	private Message currentRequest;

	private int numServers;

	private int numReplications;

	// will need to fix time of receving response
	private long time = 0;

	// all socket channels that we open for this client
	private List<SocketChannel> socketChannels;

	private ByteBuffer writeBuffer = ByteBuffer.allocate(1536);

	// main queue that is passed by messageProcessor
	private BlockingQueue<Message> queue;

	// keeps tracking of what should come at which channel
	private Map<SocketChannel, ArrayBlockingQueue<SocketChannel>> localQueue =
			new HashMap<SocketChannel, ArrayBlockingQueue<SocketChannel>>();

	// keeps tracking request states for replications
	private Map<SocketChannel, RequestState> requestStates =
			new HashMap<SocketChannel, RequestState>();

	// gives access to Message object where we track times
	// and other instrumentation stuff
	public Map<SocketChannel, Message> loggerMap;

	public AsynClient(int id, List<String> addresses, int[] ports,
			BlockingQueue<Message> queue, int numServers, int numReplications,
			Map<SocketChannel, Message> loggerMap) throws IOException {
		this.id = id;
		this.addresses = addresses;
		this.ports = ports;
		this.numServers = numServers;
		this.numReplications = numReplications;
		this.queue = queue;
		this.socketChannels = new ArrayList<SocketChannel>();
		this.loggerMap = loggerMap;

		// opens socket channels only for servers we need to replicate to
		for (int i = this.id; i < this.id + this.numReplications; i++) {
			this.socketChannels.add(this.initiateConnection(i % this.numServers));
		}

		// gives half a second to prepare connections for "finishing" them
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// finish connections
		for (int i = 0; i < this.numReplications; i++) {
			this.finishConnection(this.socketChannels.get(i));
		}
	}

	public void run() {
		while (true) {
			try {
				if (queue.peek() != null) {

					this.currentRequest = this.queue.poll();
					// finalize logging time spent in queue
					if (currentRequest.isLogged) {
						this.currentRequest.queueTime =
								(System.nanoTime() - currentRequest.queueTime) / 1000;
					}
					this.requestStates.put(this.currentRequest.socket,
							new RequestState());
					// write to all socket channels
					for (int i = 0; i < numReplications; i++) {
						this.write(this.socketChannels.get(i),
								this.currentRequest);
					}
				}

				// reads from all socket channels
				for (int i = 0; i < numReplications; i++) {
					this.read(this.socketChannels.get(i));
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// helper method to initiate socket connections
	private SocketChannel initiateConnection(int i) throws IOException {

		SocketChannel socketChannel = SocketChannel.open();
		// put channel in non blocking mode for asynchronous requests handling
		socketChannel.configureBlocking(false);

		socketChannel.connect(new InetSocketAddress(InetAddress
				.getByName(this.addresses.get(i)), this.ports[i]));

		return socketChannel;
	}

	// helper method to finish socket connections
	private void finishConnection(SocketChannel socketChannel) {
		try {
			socketChannel.finishConnect();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	private void write(SocketChannel socketChannel, Message request)
			throws IOException {
		this.writeBuffer.clear();
		this.writeBuffer = ByteBuffer.wrap(request.data);
		while (this.writeBuffer.hasRemaining()) {
			socketChannel.write(this.writeBuffer);
		}
		// start logging processing time when we write to primary server
		if (request.isLogged) {
			if (socketChannel == socketChannels.get(0)) {
				request.processingTime = System.nanoTime();
			}
		}

		// add socket channel of memaslap client to queue in order to
		// know which response to expect when reading
		ArrayBlockingQueue<SocketChannel> queue = this.localQueue
				.get(socketChannel);
		if (queue == null) {
			queue = new ArrayBlockingQueue<SocketChannel>(1000);
			this.localQueue.put(socketChannel, queue);
		}
		queue.add(request.socket);
	}

	private void read(SocketChannel socketChannel) throws IOException {

		ByteBuffer readBuffer = ByteBuffer.allocate(4096);

		int numRead;
		try {
			numRead = socketChannel.read(readBuffer);
		} catch (IOException e) {
			e.printStackTrace();
			socketChannel.close();
			return;
		}

		// it's possible that we read nothing since we are asynchronous
		if (numRead == -1 || numRead == 0) {
			return;
		}

		// if we receive more than nothing we should fix time of receiving
		// to log processing time later
		this.time = System.nanoTime();

		// now we gonna check whom to send the responses back since
		// we may receive more than 1 response in one buffer
		ArrayBlockingQueue<SocketChannel> queue = localQueue.get(socketChannel);

		// and we have to parse read data
		String[] parsed = new String(readBuffer.array()).split("\r\n");

		for (int i = 0; i < parsed.length - 1; i++) {
			SocketChannel belongsTo = queue.poll();
			RequestState currentState = requestStates.get(belongsTo);
			Message request = this.loggerMap.get(belongsTo);

			// update status of repl's made for this request
			currentState.nReplMade++;

			// if response starts with DEL then it is an answer to DELETE request
			// make delete flag true
			if (parsed[i].startsWith("DEL")) {
				currentState.delete = true;
			}

			// check if we get expected result, if not,
			// put unsuccessful replication flag
			// and unsuccessful operation flag for logging
			if (currentState.delete) {
				if (!parsed[i].startsWith("DEL")) {
					currentState.allSuccess = false;
					request.successFlag = false;
				}
			} else {
				if (!parsed[i].startsWith("STO")) {
					currentState.allSuccess = false;
					request.successFlag = false;
				}
			}

			// check if we received answers from all expected servers for this request
			if (currentState.nReplMade == this.numReplications) {

				// finalize logging of processing time
				if (request.isLogged) {
					request.processingTime =
							(time - request.processingTime) / 1000;
				}

				// if replications flag is false, send ERROR back to memaslap client
				// else send STORED or DELETED based on delete flag
				if (!currentState.allSuccess) {
					request.server.send(belongsTo, RequestState.ERROR);
				} else if (currentState.delete) {
					request.server.send(belongsTo, RequestState.DELETED);
				} else {
					request.server.send(belongsTo, RequestState.STORED);
				}
				// after sending back, erase request state
				this.requestStates.put(belongsTo, null);
			}
		}
	}
}
