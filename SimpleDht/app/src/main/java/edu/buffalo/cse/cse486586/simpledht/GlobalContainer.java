package edu.buffalo.cse.cse486586.simpledht;

import java.util.HashMap;

/**
 * Created by shivraj on 23/3/18.
 */

public class GlobalContainer {

	static String predecessor = null;
	static String successor = null;

	static boolean first = false;

	public final static HashMap<String, String> nodeName;

	static {
		nodeName = new HashMap<String, String>();
		nodeName.put("11108", "5554");
		nodeName.put("11112", "5556");
		nodeName.put("11116", "5558");
		nodeName.put("11120", "5560");
		nodeName.put("11124", "5562");
	}

	static final byte NOTICE = 0;
	static final byte REQUEST = 1;
	static final byte SHUFFLE = 2;
	static final byte INSERT = 3;

	static String requester = null;
//	static final byte SEQUENCE = 2;
}
