package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import javax.microedition.khronos.opengles.GL;

import edu.buffalo.cse.cse486586.simpledynamo.configurations.GlobalContainer;

/**
 * Created by shivraj on 14/4/18.
 */

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

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


		byte type = rec.readByte();

			switch (type) {
				case GlobalContainer.INSERT :
					ContentValues cv = new ContentValues();

					String times = rec.readUTF();
					String keySet = rec.readUTF();
					String valueSet = rec.readUTF();
					cv.put(GlobalContainer.KEY_FIELD, keySet);
					cv.put(GlobalContainer.VALUE_FIELD, valueSet);
					//Log.e(TAG, "Inserting " + cv);

					String[] keys = {keySet, null};
					cr.update(GlobalContainer.mUri, cv, keySet, null);
					Cursor resultCursor;// = cr.query(GlobalContainer.mUri, null, null, keys, null);
//					if( times.equals("FIRST") ) {
//
//						if( resultCursor == null )
//							cr.update(GlobalContainer.mUri, cv, null, null);
//						else {
//							resultCursor.moveToFirst();
//							String result = resultCursor.getString(resultCursor.getColumnIndex(GlobalContainer.VALUE_FIELD));
//							Log.e("INSERT", " the result is 1 " + result + " the key is " + keySet + " with value : " + valueSet);
//							if( !result.equals(valueSet) ) {
//								//cr.update(GlobalContainer.mUri, null, keySet, null);
//								cr.update(GlobalContainer.mUri, cv, keySet, null);
//							}
//						}
//					}
//					else if( times.equals("SECOND") ) {
//						if(resultCursor == null)
//							cr.update(GlobalContainer.mUri, cv, null, null);
//						else {
//							resultCursor.moveToFirst();
//							String result = resultCursor.getString(resultCursor.getColumnIndex(GlobalContainer.VALUE_FIELD));
//							Log.e("INSERT", " the result is 2 " + result + " the key is " + keySet + " with value : " + valueSet);
//							if( !result.equals(valueSet) ) {
//								//cr.update(GlobalContainer.mUri, null, keySet, null);
//								cr.update(GlobalContainer.mUri, cv, keySet, null);
//							}
//						}
//					}
//					else {
//						if(resultCursor == null)
//							cr.update(GlobalContainer.mUri, cv, null, null);
//						else {
//							resultCursor.moveToFirst();
//							String result = resultCursor.getString(resultCursor.getColumnIndex(GlobalContainer.VALUE_FIELD));
//							Log.e("INSERT", " the result is 3 " + result + " the key is " + keySet + " with value : " + valueSet);
//							if( !result.equals(valueSet) ) {
//								//cr.update(GlobalContainer.mUri, null, keySet, null);
//								cr.update(GlobalContainer.mUri, cv, keySet, null);
//							}
//						}
//					}

					out.write(1);
					break;

				case GlobalContainer.REQUEST:

					Log.e("REQUEST", "Request received.....!!!!!!!!!!");

					String requester = rec.readUTF();
					String selection = rec.readUTF();

					if(requester.equals(GlobalContainer.myPort)) {
						out.writeBoolean(false);
						break;
					}

					Log.e("REQUEST", "Request received from origin " + GlobalContainer.requester +" The key asked " + selection);

					String[] sArray = {selection, requester};
					resultCursor = cr.query(GlobalContainer.mUri, null, selection, sArray, null);

					Log.e("REQUEST", "The resultCursor is " + resultCursor);

					if(resultCursor == null || resultCursor.getCount() == 0) {
						out.writeBoolean(false);
						break;
					}

					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e("REQUEST", "Wrong number of rows");
					}

					int keyIndex = resultCursor.getColumnIndex(GlobalContainer.KEY_FIELD);
					int valueIndex = resultCursor.getColumnIndex(GlobalContainer.VALUE_FIELD);

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

				case GlobalContainer.RECOVERY:
					Log.e("RECOVERY", "Request received.....!!!!!!!!!!");

					requester = rec.readUTF();
					sArray = new String[]{"@@", requester};

					Log.e("RECOVERY", "Recovery received from origin " + " The requester is : " + requester);

					resultCursor = cr.query(GlobalContainer.mUri, null, "@@", sArray, null);

					Log.e("RECOVERY", "The resultCursor is " + resultCursor);

					if(resultCursor == null || resultCursor.getCount() == 0) {
						out.writeBoolean(false);
						break;
					}

					resultCursor.moveToFirst();

					if (!(resultCursor.isFirst() && resultCursor.isLast())) {
						Log.e("REQUEST", "Wrong number of rows");
					}

					keyIndex = resultCursor.getColumnIndex(GlobalContainer.KEY_FIELD);
					valueIndex = resultCursor.getColumnIndex(GlobalContainer.VALUE_FIELD);

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

				case GlobalContainer.DELETE :

					keySet = rec.readUTF();

					cr.update(GlobalContainer.mUri,null,keySet,null);

					break;
				default:
					break;
			}



		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			new ServerTask(cr).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}


		return null;
	}
}
