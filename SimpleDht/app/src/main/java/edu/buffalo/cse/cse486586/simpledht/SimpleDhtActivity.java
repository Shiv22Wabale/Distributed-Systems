package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;

public class SimpleDhtActivity extends Activity {

	static String myPort;
	static final int SERVER_PORT = 10000;

	static final String  AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";
	public static Uri mUri = null;

	private ContentResolver cr = null;
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dht_main);

		cr = getContentResolver();

		TextView tv = (TextView) findViewById(R.id.textView1);
		tv.setMovementMethod(new ScrollingMovementMethod());
		findViewById(R.id.button3).setOnClickListener(
				new OnTestClickListener(tv, getContentResolver()));

		Uri.Builder builder = new Uri.Builder();
		builder.authority(AUTHORITY);
		builder.scheme("content");
		mUri = builder.build();

		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask(cr).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();
		}

		TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));


		if(!myPort.equals("11108")){
			notifyJoining();
		}
		else {
			try {
				ServerTask.listHosts.put(ServerTask.genHash(GlobalContainer.nodeName.get("11108") ), "11108");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
	}

	private void notifyJoining(){
		new ClientTask(null).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "NOTICE", "notify", myPort);
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
		return true;
	}
}
