package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import edu.buffalo.cse.cse486586.simpledynamo.configurations.GlobalContainer;

public class SimpleDynamoActivity extends Activity {

	static final int SERVER_PORT = 10000;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		GlobalContainer.myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		ArrayList<String> sortedList = new ArrayList<String>();
		sortedList.addAll(GlobalContainer.sortedNodes);
		Collections.sort(sortedList);

		GlobalContainer.susuccessor = GlobalContainer.listHosts.get( sortedList.get( ( sortedList.indexOf(genHash(GlobalContainer.nodeName.get(GlobalContainer.myPort))) + 2 ) % 5 ) );
		GlobalContainer.successor = GlobalContainer.listHosts.get( sortedList.get( ( sortedList.indexOf(genHash(GlobalContainer.nodeName.get(GlobalContainer.myPort))) + 1 ) % 5 ) );
		GlobalContainer.predecessor = GlobalContainer.listHosts.get( sortedList.get( ( sortedList.indexOf(genHash(GlobalContainer.nodeName.get(GlobalContainer.myPort))) + 5 - 1 ) % 5 ) );
		GlobalContainer.prepredecessor = GlobalContainer.listHosts.get( sortedList.get( ( sortedList.indexOf(genHash(GlobalContainer.nodeName.get(GlobalContainer.myPort))) + 5 - 2 ) % 5 ) );

		GlobalContainer.first = genHash(GlobalContainer.nodeName.get(GlobalContainer.myPort)).compareTo( genHash(GlobalContainer.nodeName.get(GlobalContainer.predecessor)) ) < 0;

		Log.e("setPredSuc", "Successor is : " + GlobalContainer.successor + " predecessor : " + GlobalContainer.predecessor + " it's first : " + GlobalContainer.first);

		Uri.Builder builder = new Uri.Builder();
		builder.authority(GlobalContainer.AUTHORITY);
		builder.scheme("content");
		GlobalContainer.mUri = builder.build();

		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			if( new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", GlobalContainer.successor, GlobalContainer.myPort).get() == null)
				new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", GlobalContainer.susuccessor, GlobalContainer.myPort).get();
			if(new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", GlobalContainer.predecessor, GlobalContainer.myPort).get() == null)
				new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", GlobalContainer.prepredecessor, GlobalContainer.myPort).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		GlobalContainer.recovered.set(true);



//		new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", GlobalContainer.successor, GlobalContainer.myPort);
//		new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", GlobalContainer.predecessor, GlobalContainer.myPort);



//		new ParallelTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, GlobalContainer.successor, GlobalContainer.susuccessor);
//		new ParallelTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, GlobalContainer.predecessor, GlobalContainer.prepredecessor);
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
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

//	class ParallelTask extends AsyncTask< String, Void, Void> {
//
//		@Override
//		protected Void doInBackground(String... strings) {
//			try {
//				if( new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", strings[0], GlobalContainer.myPort).get() == null)
//					new ClientTask(getContentResolver()).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"RECOVERY", strings[1], GlobalContainer.myPort).get();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			} catch (ExecutionException e) {
//				e.printStackTrace();
//			}
//			return null;
//		}
//	}
}