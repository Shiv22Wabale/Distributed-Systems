package edu.buffalo.cse.cse486586.simpledynamo.configurations;

import android.net.Uri;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by shivraj on 13/4/18.
 */

public class GlobalContainer {

	public static String prepredecessor = null;
	public static String predecessor = null;
	public static String successor = null;
	public static String susuccessor = null;

	public static String requester = null;

	public static boolean first = false;

	public static String myPort;
	public static final int SERVER_PORT = 10000;

	public final static HashMap<String, String> nodeName;
	public final static CopyOnWriteArrayList<String> sortedNodes;
	public final static Map<String, String> listHosts;

	static {
		nodeName = new HashMap<String, String>();
		nodeName.put("11108", "5554");
		nodeName.put("11112", "5556");
		nodeName.put("11116", "5558");
		nodeName.put("11120", "5560");
		nodeName.put("11124", "5562");

		sortedNodes = new CopyOnWriteArrayList<String>();
		sortedNodes.add(genHash("5554"));
		sortedNodes.add(genHash("5556"));
		sortedNodes.add(genHash("5558"));
		sortedNodes.add(genHash("5560"));
		sortedNodes.add(genHash("5562"));

//		Collections.sort(sortedNodes);

		listHosts = new TreeMap<String, String>();
		listHosts.put(genHash("5554"), "11108");
		listHosts.put(genHash("5556"), "11112");
		listHosts.put(genHash("5558"), "11116");
		listHosts.put(genHash("5560"), "11120");
		listHosts.put(genHash("5562"), "11124");
	}

	private static String genHash(String input) {
		MessageDigest sha1 = null;
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	public static final byte NOTICE = 0;
	public static final byte REQUEST = 1;
	public static final byte SHUFFLE = 2;
	public static final byte INSERT = 3;
	public static final byte RECOVERY = 4;
	public static final byte DELETE = 5;

	public static final String  AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	public static Uri mUri = null;

	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";

	public static AtomicBoolean recovered = new AtomicBoolean(false);
}