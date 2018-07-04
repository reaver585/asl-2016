package ch.ethz.asltest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Circle {

	private final Map<String, Integer> keyToId =
			new HashMap<String, Integer>();
	
	private final MessageDigest hashFunction;
	
	private final int numServers;
	
	private final int numberOfReplicas;
	
	// TreeMap is used to represent a circle
	private final SortedMap<BigInteger, String> circle =
			new TreeMap<BigInteger, String>();

	public MD5Circle(int numServers, int numberOfReplicas)
			throws NoSuchAlgorithmException {

		this.hashFunction = MessageDigest.getInstance("MD5");
		this.numServers = numServers;
		this.numberOfReplicas = numberOfReplicas;

		// we have 7 random strings to promote uniform
		// positioning of virtual nodes
		List<String> nodes = new ArrayList<String>();
		nodes.add("8QAYsEvF");
		nodes.add("Hjh3-bAd");
		nodes.add("1XB7Nm7n");
		nodes.add("5fP12bRi");
		nodes.add("TdMJnpl3");
		nodes.add("GdIi-wGJ");
		nodes.add("a37pGaKH");

		// we map strings mentioned above to memcached servers id's
		for (int i = 0; i < 7; i++) {
			keyToId.put(nodes.get(i), i);
		}

		// and use only $numServers$ of those strings
		for (int i = 0; i < this.numServers; i++) {
			add(nodes.get(i));
		}
	}

	// this method adds/replicates virtual nodes to the circle
	public void add(String node) {
		for (int i = 0; i < numberOfReplicas; i++) {
			circle.put(this.hash((node + i).getBytes()), node);
		}
	}

	// this method gets id of memcached server through checking the circle
	// of hashspace
	public Integer get(byte[] key) {
		if (circle.isEmpty()) {
			return null;
		}
		BigInteger hash = this.hash(key);
		// if our hash value doesn't fall directly on one of the nodes
		// we check the first element of tailmap
		// if tailmap is empty it means we reached end of the circle
		// so we return first element of the circle
		if (!circle.containsKey(hash)) {
			SortedMap<BigInteger, String> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		// return id corresponding to that node(string) as memcached
		// server id
		return this.keyToId.get(circle.get(hash));
	}

	// md5 hasher helper method
	public BigInteger hash(byte[] key) {
		byte[] theDigest = hashFunction.digest(key);
		BigInteger hashValue = new BigInteger(1, theDigest);
		hashFunction.reset();

		return hashValue;
	}
}