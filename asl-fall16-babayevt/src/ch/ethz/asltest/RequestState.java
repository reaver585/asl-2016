package ch.ethz.asltest;

// intances of this class are used to track replication status of requests
// also 3 static fields are available to send responses back to memaslap clients
// from asynchronous client
public class RequestState {
	public int nReplMade = 0;
	public boolean allSuccess = true;
	public boolean delete = false;

	public static final byte[] ERROR = new String("ERROR\r\n").getBytes();
	public static final byte[] STORED = new String("STORED\r\n").getBytes();
	public static final byte[] DELETED = new String("DELETED\r\n").getBytes();
}