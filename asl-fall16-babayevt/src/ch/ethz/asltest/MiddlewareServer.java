package ch.ethz.asltest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogRecord;
import java.util.logging.Formatter;

public class MiddlewareServer implements Runnable {

	private String hostAddress;

	private int port;

	private ServerSocketChannel serverChannel;

	// selector is needed since we accept connections here
	private Selector selector;

	private ByteBuffer readBuffer = ByteBuffer.allocate(1536);

	// read requests will be passed to this messageProcessor
	private MessageProcessor messageProcessor;

	// our only logger
	private Logger logger = Logger.getLogger(Class.class.getName());

	// counter will make sure that only 1 of 100 get or set
	// requests will be logged
	private int getCounter = 0;
	private int setCounter = 0;

	// should have queue for changing keys since
	// keys are not thread safe in Java NIO
	private List<ChangeKey> changeKeyQueue = new LinkedList<ChangeKey>();

	// maps memaslap's client's socket to queue of responses
	// to be sent to that client
	private Map<SocketChannel, List<ByteBuffer>> queueToMemaslap = new HashMap<SocketChannel, List<ByteBuffer>>();

	// gives access to Message object where we track times
	// and other instrumentation stuff
	public Map<SocketChannel, Message> loggerMap;

	public MiddlewareServer(String hostAddress, int port,
			MessageProcessor messageProcessor,
			Map<SocketChannel, Message> loggerMap) throws IOException {
		this.hostAddress = hostAddress;
		this.port = port;
		this.selector = this.initSelector();
		this.messageProcessor = messageProcessor;
		this.loggerMap = loggerMap;
		this.logger.setLevel(Level.INFO);
		// no need to write log to console, set it false
		this.logger.setUseParentHandlers(false);
		Handler handler = new FileHandler("instrum.csv");
		handler.setFormatter(new MyCustomFormatter());
		this.logger.addHandler(handler);
		this.logger.info("t_total,t_queue,t_mcd,flag,op_type");
	}

	// this method is called by asynchronous or synchronous client
	// and adds response to queueToMemaslap
	public void send(SocketChannel socket, byte[] data) {
		synchronized (this.changeKeyQueue) {
			// since we now have something to write we change
			// key for this channel to WRITE state
			this.changeKeyQueue
					.add(new ChangeKey(socket, SelectionKey.OP_WRITE));

			synchronized (this.queueToMemaslap) {
				List<ByteBuffer> queue = (List<ByteBuffer>) this.queueToMemaslap
						.get(socket);
				if (queue == null) {
					queue = new ArrayList<ByteBuffer>();
					this.queueToMemaslap.put(socket, queue);
				}
				queue.add(ByteBuffer.wrap(data));
			}
		}
		// wakes up selector's blocking .select() method
		this.selector.wakeup();
	}

	public void run() {
		while (true) {
			try {
				// change any keys, if necessary
				synchronized (this.changeKeyQueue) {
					Iterator<ChangeKey> changes = this.changeKeyQueue
							.iterator();
					while (changes.hasNext()) {
						ChangeKey change = (ChangeKey) changes.next();
						SelectionKey key = change.socket.keyFor(this.selector);
						key.interestOps(change.ops);
					}
					this.changeKeyQueue.clear();
				}

				// blocking method which waites for some events from connections
				this.selector.select();

				// typical Java NIO iteration over selected keys for socket channels
				Iterator<SelectionKey> selectedKeys = this.selector
						.selectedKeys().iterator();
				while (selectedKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectedKeys.next();
					selectedKeys.remove();

					// key may be invalid, should check that
					if (!key.isValid()) {
						continue;
					}

					// define which action key is ready to perform and
					// perform that action
					if (key.isAcceptable()) {
						this.accept(key);
					} else if (key.isReadable()) {
						this.read(key);
					} else if (key.isWritable()) {
						this.write(key);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private Selector initSelector() throws IOException {
		// initialize selector
		Selector socketSelector = SelectorProvider.provider().openSelector();
		this.serverChannel = ServerSocketChannel.open();
		// use non blocking fashion over here
		serverChannel.configureBlocking(false);

		InetSocketAddress isa = new InetSocketAddress(
				InetAddress.getByName(this.hostAddress), this.port);
		serverChannel.socket().bind(isa);

		// register serversocketchannel with selector
		// and make it ready to accept connections
		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}

	// helper method accepts socketchannels and registers
	// them with our selector
	private void accept(SelectionKey key) throws IOException {
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
				.channel();

		SocketChannel socketChannel = serverSocketChannel.accept();
		// as usual, non blocking fashion here
		socketChannel.configureBlocking(false);

		// after registering, this socket channel goes to READ state
		// since we expect some request from it
		socketChannel.register(this.selector, SelectionKey.OP_READ);
	}

	private void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		this.readBuffer.clear();

		int numRead;
		try {
			numRead = socketChannel.read(this.readBuffer);
		} catch (IOException e) {
			// if client closed connection, we cancel key and discard
			// client's socketchannel
			key.cancel();
			socketChannel.close();
			return;
		}

		if (numRead == -1) {
			return;
		}

		byte[] dataCopy = new byte[numRead];
		System.arraycopy(this.readBuffer.array(), 0, dataCopy, 0, numRead);
		// initialize instance of Message class
		Message newRequest = new Message(this, socketChannel, dataCopy);

		// log only each 100th get and set request and
		if (dataCopy[0] != 103) {
			newRequest.isGet = false;
			this.setCounter++;
		} else {
			this.getCounter++;
		}

		// map socketchannel to message
		this.loggerMap.put(socketChannel, newRequest);

		// conditional checks it is 100th request and
		// starts logging
		if (this.getCounter % 100 == 1) {
			if (newRequest.isGet) {
				newRequest.isLogged = true;
				newRequest.responseTime = System.nanoTime();
			}
		} else if (this.setCounter % 100 == 1) {
			if (!newRequest.isGet) {
				newRequest.isLogged = true;
				newRequest.responseTime = System.nanoTime();
			}
		}

		// pass the message to our messageProcessor
		try {
			this.messageProcessor.processData(newRequest);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		// extract message instance from loggerMap
		Message request = this.loggerMap.get(socketChannel);

		// access queue to memaslap to see what we have to
		// send back to this channel
		synchronized (this.queueToMemaslap) {
			List<ByteBuffer> queue = (List<ByteBuffer>) this.queueToMemaslap
					.get(socketChannel);

			while (!queue.isEmpty()) {
				ByteBuffer buf = (ByteBuffer) queue.get(0);
				socketChannel.write(buf);
				if (buf.remaining() > 0) {
					// break in case of buffer overfill
					break;
				}
				queue.remove(0);
			}

			if (queue.isEmpty()) {
				// finalize logging response time, if we log this request
				if (request.isLogged) {
					request.responseTime = (System.nanoTime() - request.responseTime) / 1000;
					String logMsg;
					// different output for gets and sets
					if (request.isGet) {
						logMsg = String.format("%1$s,%2$s,%3$s,%4$s",
								request.responseTime, request.queueTime,
								request.processingTime,
								String.valueOf(request.successFlag)) + Message.get;
					} else {
						logMsg = String.format("%1$s,%2$s,%3$s,%4$s",
								request.responseTime, request.queueTime,
								request.processingTime,
								String.valueOf(request.successFlag)) + Message.set;
					}
					this.logger.info(logMsg);
				}
				// after writing data, get ready to listen to that channel again
				key.interestOps(SelectionKey.OP_READ);
			}
		}
	}

	private static class MyCustomFormatter extends Formatter {
	  @Override
	  public String format(LogRecord record) {
	    StringBuffer sb = new StringBuffer();
	    sb.append(record.getMessage());
	    sb.append("\n");
	    return sb.toString();
    }
	}
}
