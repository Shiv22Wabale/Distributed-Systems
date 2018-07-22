package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.CursorIndexOutOfBoundsException;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.GuardedObject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by shivraj on 23/3/18.
 */

class ServerTask extends AsyncTask<ServerSocket, String, Void> {

	static final String TAG = ServerTask.class.getSimpleName();
	//static CopyOnWriteArrayList<String> listHosts = new CopyOnWriteArrayList<String>();
	static Map<String, String> listHosts = new TreeMap<String, String>();

	ContentResolver cr = null;

	public ServerTask( ContentResolver cr ){
		this.cr = cr;
	}

	@Override
	protected Void doInBackground(ServerSocket... sockets) {
		ServerSocket serverSocket = sockets[0];

		try {
			Socket receiver = serverSocket.accept();

			DataInputStream rec = new DataInputStream(receiver.getInputStream());

			DataOutputStream out = new DataOutputStream(receiver.getOutputStream());

			Log.d("Success", "Message Success");


			byte type = rec.readByte();
			switch (type) {
				case GlobalContainer.NOTICE:
					Log.e(TAG, "Received noticed");
					publishProgress(rec.readUTF(), rec.readUTF());
					break;
				case GlobalContainer.INSERT:

//					String key = rec.readUTF();
//					String value = rec.readUTF();
					//getContentResolver().
					//Cursor resultCursor = cr.query(SimpleDhtActivity.mUri, null, selection, null, null);
					ContentValues cv = new ContentValues();

					cv.put(OnTestClickListener.KEY_FIELD, rec.readUTF());
					cv.put(OnTestClickListener.VALUE_FIELD, rec.readUTF());
					Log.e(TAG, "Inserting " + cv);

					cr.insert(SimpleDhtActivity.mUri, cv);

					break;
				case GlobalContainer.REQUEST:

					Log.e("REQUEST", "Request received.....!!!!!!!!!!");

					GlobalContainer.requester = rec.readUTF();
					String selection = rec.readUTF();
					if(GlobalContainer.requester.equals(SimpleDhtActivity.myPort)) {
						out.writeBoolean(false);
						break;
					}

					Log.e("REQUEST", "Request received from origin " + GlobalContainer.requester +" The key asked " + selection);

					//getContentResolver().
					//simpleDhtProvider = new SimpleDhtProvider();
					//Cursor resultCursor = simpleDhtProvider.queryRing(SimpleDhtActivity.mUri, null, selection, null, null);
					String[] sArray = {selection};
					Cursor resultCursor = cr.query(SimpleDhtActivity.mUri, null, selection, sArray, null);


					Log.e("REQUEST", "The resultCursor is " + resultCursor);

					if(resultCursor == null || resultCursor.getCount() == 0) {
						out.writeBoolean(false);
						break;
					}


					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e(TAG, "Wrong number of rows");
					}

					int keyIndex = resultCursor.getColumnIndex(OnTestClickListener.KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(OnTestClickListener.VALUE_FIELD);

					while ( !resultCursor.isAfterLast() ) {
						out.writeBoolean(true);
						String key = resultCursor.getString(keyIndex);
						String result = resultCursor.getString(valueIndex);

						out.writeUTF(key);
						out.writeUTF(result);

						resultCursor.moveToNext();

						Log.e("REQUEST", "Returning the msg recursively " + key + " : " + result);
					}

					out.writeBoolean(false);

					break;

				case GlobalContainer.SHUFFLE:

					int size = rec.readInt();
					for(int i = 0; i < size; i++) {
						String key = rec.readUTF();
						String value = rec.readUTF();

						ContentValues contentValues = new ContentValues();
						contentValues.put(OnTestClickListener.KEY_FIELD, key);
						contentValues.put(OnTestClickListener.VALUE_FIELD, value);
						cr.insert(SimpleDhtActivity.mUri, contentValues);
					}
					break;
				default:
					break;
			}

			new ServerTask(cr).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (UnknownHostException e) {
			Log.e(TAG, "ServerTask UnknownHostException"+e.getMessage());
		} catch (IOException e) {
			Log.e(TAG, "ServerTask socket IOException "+e.getMessage());
		}

		return null;
	}

	@Override
	protected void onProgressUpdate(String... values) {
		//super.onProgressUpdate(values);
		try {

			Log.e(TAG, values[0] + " notice by " + values[1]);
			if(values[0].equals("notify")) {

				Log.e("HASH", GlobalContainer.nodeName.get(values[1]) + " the gen of this is " + genHash(GlobalContainer.nodeName.get(values[1])) + " ---- The value is " +  values[1]);
				listHosts.put(genHash(GlobalContainer.nodeName.get(values[1])), values[1]);

				ArrayList<String> nodes = new ArrayList<String>(listHosts.keySet());

				//shuffleMessages(values[1]);
				//setPredSuc(values[1]);
				int size = nodes.size();
				for(int i = 0; i < size; i++) {

					Log.e("setPredSuc",nodes.get(i) + " " + listHosts.get(nodes.get(i)));

					if(i != 0 )
						new ClientTask(null).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "NOTICE", "notifyAll", listHosts.get(nodes.get(i - 1)) , listHosts.get(nodes.get(i)) );
					else
						new ClientTask(null).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "NOTICE", "notifyAll", listHosts.get(nodes.get(size - 1)) , listHosts.get(nodes.get(i)) );
				}
			}
			else if(values[0].equals("notifyAll")) {
				//shuffleMessages(values[1]);
				//setPredSuc(values[1]);
				//String[] v = values[1].split("|");
				//GlobalContainer.successor = values[1];

				String previous = GlobalContainer.predecessor;
				GlobalContainer.predecessor = values[1];
				//GlobalContainer.last = Boolean.parseBoolean(v[1]);
				//if( genHash(GlobalContainer.nodeName.get(SimpleDhtActivity.myPort)).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.successor))) > 0 )

				GlobalContainer.first = genHash(GlobalContainer.nodeName.get(SimpleDhtActivity.myPort)).compareTo( genHash(GlobalContainer.nodeName.get(GlobalContainer.predecessor)) ) < 0;

				if( !SimpleDhtProvider.keyList.isEmpty() && !previous.equals(GlobalContainer.predecessor)) {
					Cursor resultCursor = cr.query(SimpleDhtActivity.mUri, null, "@", null, null);

					if( ! ( resultCursor == null || resultCursor.getCount() == 0) ) {
						resultCursor.moveToFirst();

						if (!(resultCursor.isFirst() && resultCursor.isLast())) {
							Log.e(TAG, "Wrong number of rows");
						}



						int keyIndex = resultCursor.getColumnIndex(OnTestClickListener.KEY_FIELD);
						int valueIndex = resultCursor.getColumnIndex(OnTestClickListener.VALUE_FIELD);


						while ( !resultCursor.isAfterLast() ) {
							String key = resultCursor.getString(keyIndex);
							String result = resultCursor.getString(valueIndex);

							ContentValues cv = new ContentValues();

							cv.put(OnTestClickListener.KEY_FIELD, key);
							cv.put(OnTestClickListener.VALUE_FIELD, result);

							Log.e(TAG, "SHUFFLE " + cv);

							cr.delete(SimpleDhtActivity.mUri, key, null);
							cr.insert(SimpleDhtActivity.mUri, cv);

							resultCursor.moveToNext();

							Log.e(TAG, "SHUFFLE the msg recursively " + key + " : " + result);
						}
					}
				}

				Log.e("setPredSuc", "predecessor is :  " + GlobalContainer.predecessor);
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	private void shuffleMessages(String newNode) throws NoSuchAlgorithmException {

		if( genHash(GlobalContainer.successor).compareTo(newNode) > 0) {

			HashMap<String, String> msgSet = new HashMap<String, String>();

			MatrixCursor resultCursor = (MatrixCursor)cr.query(SimpleDhtActivity.mUri, null, "@", null, null);
			if(resultCursor == null)
				return;
			int count = resultCursor.getCount();
			for(int i = 0; i < count; i++) {
				String returnKey = resultCursor.getString(resultCursor.getColumnIndex(OnTestClickListener.KEY_FIELD));
				String returnValue = resultCursor.getString(resultCursor.getColumnIndex(OnTestClickListener.VALUE_FIELD));


				if(genHash(returnKey).compareTo(newNode) > 0) {
					cr.delete(SimpleDhtActivity.mUri, returnKey, null);
					msgSet.put(returnKey, returnValue);
				}

				resultCursor.moveToNext();
			}

			if(!msgSet.isEmpty())
				new ClientTask(msgSet).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"SHUFFLE", newNode);
		}
	}

	private void setPredSuc(String servers) throws NoSuchAlgorithmException {


		String max = GlobalContainer.successor, min = GlobalContainer.predecessor;

		Log.e("setPredSuc", servers + " changing Before ------ Successor " + max + " predecessor" + min);

		String maxHash = genHash(max), minHash = genHash(min);
		//String servers = values[1];
		String hash = genHash(servers);


		if( maxHash.compareTo(hash) > 0) {
			maxHash = hash;
			max = servers;
		}

		if( minHash.compareTo(hash) < 0) {
			minHash = hash;
			min = servers;
		}

		GlobalContainer.predecessor = min;
		GlobalContainer.successor = max;

		Log.e("setPredSuc", " Successor " + max + " predecessor" + min);

	}

	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}