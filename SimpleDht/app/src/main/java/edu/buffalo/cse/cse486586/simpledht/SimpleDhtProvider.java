package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

	static final String TAG = SimpleDhtProvider.class.getSimpleName();
	static CopyOnWriteArrayList<String> keyList = new CopyOnWriteArrayList<String>();
	//static CopyOnWriteArrayList<String> keyList = new CopyOnWriteArrayList<String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@") || selection.equals("*"))
			return deleteAll();

		if(!keyList.contains(selection))
			return 1;
		keyList.remove(selection);
		File file = new File(selection);
		if(file.delete())
			return 0;
		else
			return 1;
	}

	private int deleteAll(){
		if(keyList.isEmpty())
			return 1;
		int deleted = 0;
		for(String selection : keyList) {
			File file = new File(selection);
			if(file.delete())
				deleted++;
		}
		return deleted;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	boolean checkInChord( String keySet ) throws NoSuchAlgorithmException {

//		if(GlobalContainer.successor == null)
//			return true;
//
//		if( genHash(GlobalContainer.nodeName.get(SimpleDhtActivity.myPort)).compareTo(genHash(keySet)) < 0 && ( genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.successor))) < 0 || GlobalContainer.last ) )
//				return true;
//		else if(genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.successor))) < 0 && GlobalContainer.last )
//			return true;

		if(GlobalContainer.predecessor == null)
			return true;
		if( genHash(GlobalContainer.nodeName.get(SimpleDhtActivity.myPort)).compareTo(genHash(keySet)) > 0 && ( genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.predecessor))) > 0 || GlobalContainer.first ) )
				return true;
		else if(genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.predecessor))) > 0 && GlobalContainer.first )
			return true;


//		if( genHash(GlobalContainer.nodeName.get(SimpleDhtActivity.myPort)).compareTo(genHash(keySet)) > 0 && ( genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.successor))) > 0 || GlobalContainer.last ) )
//			return true;
//		else if(genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.successor))) > 0 && GlobalContainer.last )
//			return true;

		return false;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		//return null;
		String keySet = values.getAsString("key");

		FileOutputStream outputStream;

		try {
			//if( GlobalContainer.successor == null || ( ( genHash(GlobalContainer.nodeName.get(SimpleDhtActivity.myPort)).compareTo(genHash(keySet)) < 0 || genHash(GlobalContainer.nodeName.get(SimpleDhtActivity.myPort)).compareTo(genHash(keySet)) < 0 ) &&  ( genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.successor))) < 0 || GlobalContainer.last ) ) ) {
			//if( GlobalContainer.successor == null || ( genHash(keySet).compareTo(genHash(GlobalContainer.nodeName.get(GlobalContainer.successor))) < 0 ) ) {
			if(checkInChord(keySet)) {
				outputStream = getContext().openFileOutput(values.getAsString("key"), Context.MODE_PRIVATE);

				outputStream.write(values.getAsString("value").getBytes());
				outputStream.close();
				keyList.add(keySet);

				Log.e("INSERT", "Message inserted : " + keySet);
			}
			else {
				Log.e(TAG, "Sending the msgs to " + GlobalContainer.predecessor + "Msg is " + keySet + "  with hash code : " + genHash(keySet) + " compare with " + genHash(GlobalContainer.nodeName.get(GlobalContainer.predecessor) ) + " I am first : " + GlobalContainer.first);
				//Log.e(TAG, "Sending the msgs to " + GlobalContainer.successor + "Msg is " + keySet + " " + values.getAsString("value") + "  with hash code : " + genHash(keySet));
				new ClientTask(null).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", keySet, values.getAsString("value"));
			}
		} catch (FileNotFoundException e) {
			Log.e("GRoupMessengerProvider", "File not found ......." + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			Log.e("GRoupMessengerProvider", "File write failed    " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e){
			Log.e("GRoupMessengerProvider", "Something went wrong    " + e.getMessage());
			e.printStackTrace();
		}
		//Log.v("insert", values.toString());
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder) {
		// TODO Auto-generated method stub
		//return null;
		//Log.i("GRoupMessengerProvider","The key is +  : " + selection);

		if (selection.equals("@") || ( GlobalContainer.predecessor == null && selection.equals("*")) ) {
			Log.e("REQUEST", "Return all local");
			return allLocal();
		}
		if (selection.equals("*")) {
			Log.e("REQUEST", "Return all in ring...");
			return CursorAppend(allLocal(), askPredecessor("*@", true));
		}

		if(selectionArgs != null && selectionArgs[0].equals("*@") ) {//|| selection.equals("*")){
			return CursorAppend(allLocal(), askPredecessor("*@", false));
		}

		try {

			if(keyList.contains(selection)) {
				FileInputStream inputStream;
				//int readByte;
				BufferedReader bufferedReader;

				String stringRead;

				inputStream = getContext().openFileInput(selection);

				bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

				String[] columnNames = {"key", "value"};
				//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html
				MatrixCursor matrixCursor = new MatrixCursor(columnNames);

				//String str = new String(bytes, "UTF-8");


				while ((stringRead = bufferedReader.readLine()) != null) {

					//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html
					matrixCursor.addRow(new String[]{selection, stringRead});
				}

				inputStream.close();

				return matrixCursor;
			}
			else {
				return askPredecessor(selection, true);
			}

		} catch (Exception e) {
			e.printStackTrace();
			Log.e("REQUEST", "Query failed + file is  : " + selection);
		}
		//Log.v("query", selection);
		return null;
	}

	private Cursor CursorAppend(Cursor local, Cursor pre){
		//Log.e("REQUEST", "Lenghts local : " + local.getCount() + " pre : " + pre.getCount());
		Cursor[] resultCursors = {local, pre};

		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);

		for(int i = 0; i < 2; i++) {
			Cursor resultCursor = resultCursors[i];

			Log.e("REQUEST", i + " - " + resultCursor);
			if(resultCursor == null)
				continue;
			else if(resultCursor.getCount() == 0)
				continue;

			Log.e("REQUEST", "Lenghts local : " + resultCursor.getCount() );

			resultCursor.moveToFirst();

			if (!(resultCursor.isFirst() && resultCursor.isLast())) {
				Log.e(TAG, "Wrong number of rows");
				//resultCursor.close();
				//break;
			}

			int keyIndex = resultCursor.getColumnIndex(OnTestClickListener.KEY_FIELD);
			int valueIndex = resultCursor.getColumnIndex(OnTestClickListener.VALUE_FIELD);

			while (!resultCursor.isAfterLast()) {

				String key = resultCursor.getString(keyIndex);
				String result = resultCursor.getString(valueIndex);

				matrixCursor.addRow(new String[]{key, result});

				resultCursor.moveToNext();

				Log.e("REQUEST", "Adding to result... " + key + " : " + result);
			}
		}

		Log.e("REQUEST", "Lenghts appended : " + matrixCursor.getCount());

		if(matrixCursor.getCount() == 0)
			return  null;
		return  matrixCursor;
	}

	private Cursor askPredecessor(String selection, boolean origin) {
		String[] columnNames = {"key", "value"};
		Cursor cursor = null;

		FileInputStream inputStream;
		//int readByte;
		BufferedReader bufferedReader;

		String stringRead;

//		if(keyList.isEmpty())
//			return null;

		Log.e("REQUEST","Sent REQUEST to " + GlobalContainer.predecessor + " asking for " + selection);
		try {

//			if(selection.equals("*@"))
//				cursor = (new ClientTask(null).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"REQUEST", GlobalContainer.predecessor, selection)).get();
//			else
			if(origin)
				cursor = (new ClientTask(null).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"REQUEST", SimpleDhtActivity.myPort, selection)).get();
			else
				cursor = (new ClientTask(null).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"REQUEST", GlobalContainer.requester, selection)).get();

			//matrixCursor = new MatrixCursor(columnNames);
			//matrixCursor.addRow(new String[]{selection, result});

			Log.e("REQUEST", "Found from predecessor " + cursor);

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}


		return cursor;
	}

	private Cursor allLocal() {
		Log.e("REQUET", "Asking for all local...");

		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);

		FileInputStream inputStream;
		//int readByte;
		BufferedReader bufferedReader;

		String stringRead;

		if(keyList.isEmpty())
			return null;

		List<String> localList = new ArrayList<String>();
		for(String key : keyList) {
			localList.add(key);
		}

		Collections.sort(localList, new Comparator<String>() {
			@Override
			public int compare(String lhs, String rhs) {
				try {
					return genHash(lhs).compareTo(genHash(rhs));
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				return 0;
			}
		});

		for(String selection : localList) {
			try {
				inputStream = getContext().openFileInput(selection);


				bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

				while((stringRead=bufferedReader.readLine()) != null){

					//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html

					matrixCursor.addRow(new String[]{selection, stringRead});
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				Log.e("REQUEST", "Query failed + file is  : 1" + selection);
			} catch (IOException e) {
				e.printStackTrace();
				Log.e("REQUEST", "Query failed + file is  : 2" + selection);
			}

		}

		return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
