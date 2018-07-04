package ch.ethz.asltest;

import java.io.IOException;
import java.util.concurrent.*;

// this helper class initiates $numThreadPTP$ instances of SynClient, creating a ThreadPool
public class SynThreadPool {

	public SynThreadPool(int numThreadPTP, String address, int port,
			BlockingQueue<Message> queue) {
		for (int i = 0; i < numThreadPTP; i++) {
			try {
				new Thread(new SynClient(address, port, queue)).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}