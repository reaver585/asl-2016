package ch.ethz.asltest;

import java.util.List;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class MyMiddleware implements Runnable {

	private String myIp;

	private int myPort;

	private List<String> mcAddresses;

	private int numThreadsPTP;

	private int writeToCount;

	public MyMiddleware(String myIp, int myPort, List<String> mcAddresses,
			int numThreadsPTP, int writeToCount) {
		this.myIp = myIp;
		this.myPort = myPort;
		this.mcAddresses = mcAddresses;
		this.numThreadsPTP = numThreadsPTP;
		this.writeToCount = writeToCount;
;	}

	public void run() {
		// initialize messageProcessor and mwServer and start thread for them
		// these are first actions of our MiddleWare
		try {
			MessageProcessor messageProcessor = new MessageProcessor(
					mcAddresses, numThreadsPTP, writeToCount);
			MiddlewareServer mwServer = new MiddlewareServer(myIp, myPort,
					messageProcessor, messageProcessor.loggerMap);
			new Thread(mwServer, "ServerThread").start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
}