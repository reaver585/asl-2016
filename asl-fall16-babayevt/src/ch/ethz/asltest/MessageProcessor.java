package ch.ethz.asltest;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.security.*;
import java.io.IOException;

public class MessageProcessor {

	private List<String> addresses;

	private int[] ports;

	private int numServers;

	private int numThreadsPTP;

	private int writeToCount;

	// next 2 fields may help to track balancing of requests
	private int[] assignmentTracker;

	private int counter = 0;

	// our object that will do hashing
	private MD5Circle hasher;

	// main queues for SET and GET requests
	private List<BlockingQueue<Message>> setQueues;

	private List<BlockingQueue<Message>> getQueues;

	// main clients for asynchronous and synchronous operations
	private List<AsynClient> asynClients;

	private List<SynThreadPool> synClients;

	// instance of loggerMap is created in this class and then passed
	// to mwServer and asynchronous client
	public Map<SocketChannel, Message> loggerMap = new ConcurrentHashMap<SocketChannel, Message>();

	public MessageProcessor(List<String> mcAddresses, int numThreadsPTP,
			int writeToCount) throws NoSuchAlgorithmException {

		this.numServers = mcAddresses.size();

		this.ports = new int[numServers];

		this.addresses = new ArrayList<String>();

		this.hasher = new MD5Circle(numServers, 20);

		// parse mcAddresses to separate server addresses and ports
		for (int i = 0; i < numServers; i++) {
			String[] splitted = mcAddresses.get(i).split(":");
			this.addresses.add(splitted[0]);
			this.ports[i] = Integer.parseInt(splitted[1]);
		}

		this.numThreadsPTP = numThreadsPTP;

		this.writeToCount = writeToCount;

		this.setQueues = new ArrayList<BlockingQueue<Message>>();
		this.getQueues = new ArrayList<BlockingQueue<Message>>();

		this.asynClients = new ArrayList<AsynClient>();
		this.synClients = new ArrayList<SynThreadPool>();

		for (int i = 0; i < numServers; i++) {

			this.setQueues.add(new ArrayBlockingQueue<Message>(5000));
			this.getQueues.add(new ArrayBlockingQueue<Message>(5000));

			try {

				// start async clients
				this.asynClients.add(new AsynClient(i, this.addresses,
						this.ports, this.setQueues.get(i), this.numServers,
						this.writeToCount, loggerMap));

				// dedicate 1 thread to each
				new Thread(this.asynClients.get(i), "AsynClientThread" + i)
						.start();

				// start threadpools for sync clients
				this.synClients.add(new SynThreadPool(this.numThreadsPTP,
						this.addresses.get(i), this.ports[i], this.getQueues
								.get(i)));

			} catch (IOException e) {
				System.err.println(e);
			}
		}

		this.assignmentTracker = new int[this.numServers];

	}

	public void processData(Message request) throws NoSuchAlgorithmException {

		// assignment of key to memcached server through hashing
		int assignedTo;

		// if our request starts with letter "d" then it a delete request
		// and key is located on positions 7:22 of byte array
		if (request.data[0] == 100) {
			assignedTo = this.hasher.get(Arrays
					.copyOfRange(request.data, 7, 23));
		// else, key is on positions 4:19 and we hash these entries
		} else {
			assignedTo = this.hasher.get(Arrays
					.copyOfRange(request.data, 4, 20));
		}

		// this piece of code may be used to check how load is balanced
		// uncomment to use

		// ----------------------------------------------------------
		// LOAD BALANCING CHECKER BEGIN
		// ----------------------------------------------------------

		/*
		 *
		 * this.assignmentTracker[assignedTo]++;
		 *
		 * if (this.counter % 100 == 0) { for (int i = 0; i < this.numServers;
		 * i++) { if (i == this.numServers-1) {
		 * System.out.println(this.assignmentTracker[i]); } else {
		 * System.out.print(this.assignmentTracker[i] + "\t"); } } }
		 *
		 * this.counter++;
		 */

		// ----------------------------------------------------------
		// LOAD BALANCING CHECKER END
		// ----------------------------------------------------------

		// after obtaining assignment we want to know if we deal
		// with "get" or any other ("set", "delete") request
		// if first letter is "g", we put to queue for sync client
		// else, into async queue
		// before doing that we also start logging queue time
		if (request.data[0] == 103) {

			if (request.isLogged) {
				request.queueTime = System.nanoTime();
			}
			getQueues.get(assignedTo).add(request);

		} else {

			if (request.isLogged) {
				request.queueTime = System.nanoTime();
			}
			setQueues.get(assignedTo).add(request);

		}
	}
}
