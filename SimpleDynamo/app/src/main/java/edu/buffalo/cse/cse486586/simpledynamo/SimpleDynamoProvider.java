package edu.buffalo.cse.cse486586.simpledynamo;

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
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import edu.buffalo.cse.cse486586.simpledynamo.configurations.GlobalContainer;

public class SimpleDynamoProvider extends ContentProvider {

	static CopyOnWriteArrayList<String> keyList = new CopyOnWriteArrayList<String>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.e("Delete is called", " deleting..." + selection);
		if(selection.equals("@") || selection.equals("*"))
			return deleteAll();

		String coordinator = getCoordinator(selection, 1);
		String sucessor2 = getCoordinator(selection, 2);
		String sucessor3 = getCoordinator(selection, 3);

		if(!keyList.contains(selection)) {
			if( !coordinator.equals(GlobalContainer.myPort) )
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE", coordinator, selection);
			if( !sucessor2.equals(GlobalContainer.myPort) )
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE", sucessor2, selection);
			if( !sucessor3.equals(GlobalContainer.myPort) )
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE", sucessor3, selection);
			return 1;
		}

		while(keyList.contains(selection))
			keyList.remove(selection);
		File file = new File(selection);

		if( !coordinator.equals(GlobalContainer.myPort) )
			new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE", coordinator, selection);
		if( !sucessor2.equals(GlobalContainer.myPort) )
			new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE", sucessor2, selection);
		if( !sucessor3.equals(GlobalContainer.myPort) )
			new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DELETE", sucessor3, selection);

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


	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		try {
			String keySet = values.getAsString("key");

			FileOutputStream outputStream;

			String coordinator = getCoordinator(keySet, 1);
			String sucessor2 = getCoordinator(keySet, 2);
			String sucessor3 = getCoordinator(keySet, 3);

			//Log.e("", );

			if(coordinator.equals(GlobalContainer.myPort)) {
				Log.e("INSERTING","in coordinator with key : " + keySet + " the coordinator 1 is " +  coordinator + "with value " + values.getAsString("value"));

				outputStream = getContext().openFileOutput(values.getAsString("key"), Context.MODE_PRIVATE);
				outputStream.write(values.getAsString("value").getBytes());
				outputStream.close();
				if(!keyList.contains(keySet))
					keyList.add(keySet);

				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", sucessor2, "SECOND", keySet, values.getAsString("value"));
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", sucessor3, "THIRD", keySet, values.getAsString("value"));
			}
			else if(sucessor2.equals(GlobalContainer.myPort)) {
				Log.e("INSERTING","in coordinator with key : " + keySet + " the coordinator 2 is " +  sucessor2 + "with value " + values.getAsString("value"));

				outputStream = getContext().openFileOutput(values.getAsString("key"), Context.MODE_PRIVATE);
				outputStream.write(values.getAsString("value").getBytes());
				outputStream.close();

				if(!keyList.contains(keySet))
					keyList.add(keySet);

				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", coordinator, "FIRST", keySet, values.getAsString("value"));
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", sucessor3, "THIRD", keySet, values.getAsString("value"));
			}
			else if(sucessor3.equals(GlobalContainer.myPort)) {
				Log.e("INSERTING","in coordinator with key : " + keySet + " the coordinator 3 is " +  sucessor3 + "with value " + values.getAsString("value"));

				outputStream = getContext().openFileOutput(values.getAsString("key"), Context.MODE_PRIVATE);
				outputStream.write(values.getAsString("value").getBytes());
				outputStream.close();
				if(!keyList.contains(keySet))
					keyList.add(keySet);

				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", coordinator, "FIRST", keySet, values.getAsString("value"));
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", sucessor2, "SECOND", keySet, values.getAsString("value"));
			}
			else {
				Log.e("INSERTING","in coordinator with key : " + keySet + " the coordinator not this " +  sucessor3  + "with value " + values.getAsString("value"));
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", coordinator, "FIRST", keySet, values.getAsString("value"));
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", sucessor2, "SECOND", keySet, values.getAsString("value"));
				new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"INSERT", sucessor3, "THIRD", keySet, values.getAsString("value"));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		return false;
	}

	String getCoordinator( String keySet, int index) {

		String genKey = genHash(keySet);

		List<String> sortedList = new ArrayList<String>();

		sortedList.addAll(GlobalContainer.sortedNodes);

		sortedList.add(genKey);
		Collections.sort(sortedList);

		String nodeHash = sortedList.get(  ( sortedList.indexOf(genKey) + index ) % 6 );

		//GlobalContainer.sortedNodes.remove(genHash(keySet));

		return GlobalContainer.listHosts.get( nodeHash );
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Log.e("QUERY","Asking for this key : " + selection);
			try {
				if (selectionArgs != null) {
					selection = selectionArgs[0];
					String requester = selectionArgs[1];

					if (selectionArgs[0].equals("*@")) {
						return CursorAppend(allLocal(null), askCoordinator("*@", false, requester));
					}
					if (selectionArgs[0].equals("@@")) {
						return allLocal(requester);
					} else if (keyList.contains(selection)) {
						FileInputStream inputStream;
						//int readByte;
						BufferedReader bufferedReader;

						String stringRead;

						inputStream = getContext().openFileInput(selection);

						bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

						String[] columnNames = {"key", "value"};
						//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html
						MatrixCursor matrixCursor = new MatrixCursor(columnNames);

						while ((stringRead = bufferedReader.readLine()) != null) {
							//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html
							matrixCursor.addRow(new String[]{selection, stringRead});
						}

						inputStream.close();

						return matrixCursor;
					} else if(!GlobalContainer.recovered.get()) {
						if(GlobalContainer.myPort.equals(getCoordinator(selection, 1)))
							return (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", getCoordinator(selection, 2), requester, selection)).get();
						if(GlobalContainer.myPort.equals(getCoordinator(selection, 2)))
							return (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", getCoordinator(selection, 3), requester, selection)).get();
						if(GlobalContainer.myPort.equals(getCoordinator(selection, 3)))
							return (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", getCoordinator(selection, 1), requester, selection)).get();
					}
					else
						return null;
				} else if (selection.equals("@")) {
					Log.e("REQUEST", "Return all local");
					return allLocal(null);
				} else if (selection.equals("*")) {
					Log.e("REQUEST", "Return all in ring...");
					return CursorAppend(allLocal(null), askCoordinator("*@", true, GlobalContainer.myPort));
				} else if (keyList.contains(selection)) {
					FileInputStream inputStream;

					BufferedReader bufferedReader;
					String stringRead;
					inputStream = getContext().openFileInput(selection);
					bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

					String[] columnNames = {"key", "value"};
					//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html
					MatrixCursor matrixCursor = new MatrixCursor(columnNames);

					while ((stringRead = bufferedReader.readLine()) != null) {
						//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html
						Log.e("QUERY", "The value found for the key " + selection + " the value is " + stringRead);
						matrixCursor.addRow(new String[]{selection, stringRead});
					}

					inputStream.close();

					return matrixCursor;
				} else if(GlobalContainer.myPort.equals(getCoordinator(selection, 1)) && !GlobalContainer.recovered.get()) {
					if(GlobalContainer.myPort.equals(getCoordinator(selection, 1)))
						return (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", getCoordinator(selection, 2), GlobalContainer.myPort, selection)).get();
					if(GlobalContainer.myPort.equals(getCoordinator(selection, 2)))
						return (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", getCoordinator(selection, 3), GlobalContainer.myPort, selection)).get();
					if(GlobalContainer.myPort.equals(getCoordinator(selection, 3)))
						return (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", getCoordinator(selection, 1), GlobalContainer.myPort, selection)).get();
				} else {
					return askCoordinator(selection, true, GlobalContainer.myPort);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

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

			int keyIndex = resultCursor.getColumnIndex(GlobalContainer.KEY_FIELD);
			int valueIndex = resultCursor.getColumnIndex(GlobalContainer.VALUE_FIELD);

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

	private Cursor allLocal(String requester) {
		Log.e("REQUET", "Asking for all local...");

		String[] columnNames = {"key", "value"};
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);

		FileInputStream inputStream;
		//int readByte;
		BufferedReader bufferedReader;

		String stringRead;

		if(keyList.isEmpty()) {
			Log.e("REQUEST", "The key list is empty...");
			return null;
		}

		List<String> localList = new ArrayList<String>(keyList);
//		for(String key : keyList) {
//			localList.add(key);
//		}

//		Collections.sort(localList, new Comparator<String>() {
//			@Override
//			public int compare(String lhs, String rhs) {return genHash(lhs).compareTo(genHash(rhs));}
//		});

		for(String selection : localList) {
			try {
				inputStream = getContext().openFileInput(selection);


				bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

				while((stringRead=bufferedReader.readLine()) != null){

					//Ref : https://developer.android.com/reference/android/database/MatrixCursor.html
					if(requester == null) {
						Log.e("REQUEST", "Adding " + selection + " to all ...");
						matrixCursor.addRow(new String[]{selection, stringRead});
					}
					else if( requester.equals(getCoordinator(selection, 1)) || requester.equals(getCoordinator(selection, 2)) || requester.equals(getCoordinator(selection, 3)) ) {
						Log.e("RECOVERY", "Adding " + selection + " to all ...");
						matrixCursor.addRow(new String[]{selection, stringRead});
					}
//					if(requester != null)
//						Log.e("RECOVERY", "For the requester " + requester);
//
//					Log.e("REQUEST", "Adding " + selection + " to all ...");
//					matrixCursor.addRow(new String[]{selection, stringRead});
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

	private Cursor askCoordinator(String selection, boolean origin, String requester) {
		Cursor cursor = null;

		try {

			if(origin) {
				if(selection.equals("*@")) {
					Log.e("REQUEST","Sent REQUEST to 1 " + GlobalContainer.predecessor + " asking for " + selection);

					cursor = (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"REQUEST", GlobalContainer.predecessor, requester, selection)).get();
					if(cursor == null)
						cursor = (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"REQUEST", GlobalContainer.prepredecessor, requester, selection)).get();
				} else {

					int i = 1;
					while ( ( cursor == null || cursor.getCount() == 0 ) && i <= 3) {
						Log.e("REQUEST","Sent REQUEST to 2 " + getCoordinator(selection, i) + " asking for " + selection);

						cursor = (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", getCoordinator(selection, i), requester, selection)).get();
						i++;
					}
				}
			} else {
				Log.e("REQUEST","Sent REQUEST to 3 " + GlobalContainer.predecessor + " asking for " + selection);
				cursor = (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", GlobalContainer.predecessor, requester, selection)).get();
				if(cursor == null)
					cursor = (new ClientTask(getContext().getContentResolver()).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REQUEST", GlobalContainer.prepredecessor, requester, selection)).get();
			}

			Log.e("REQUEST", "Found from Coordinator " + cursor);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		synchronized (this) {
			try {
				if (values != null) {

					String keySet = values.getAsString("key");
					FileOutputStream outputStream;
					String coordinator = getCoordinator(keySet, 1);
					String sucessor2 = getCoordinator(keySet, 2);
					String sucessor3 = getCoordinator(keySet, 3);

					if (selection != null) {
						if(selection.equals("RECOVERY")) {
							Log.e("RECOVERY"," It's going on the checking if key exists " + keySet + " exists : " + keyList.contains(keySet));
							if (keyList.contains(keySet))
								return 1;
						}
						else {
							Log.e("Delete called in update", " deleting to update it..." + selection);
							if (selection.equals("@") || selection.equals("*"))
								return deleteAll();

							if (keyList.contains(selection)) {
								File file = new File(selection);

								file.delete();
							}
						}
					}


					if (coordinator.equals(GlobalContainer.myPort)) {
						Log.e("UPDATING", "in coordinator with key : " + keySet + " the coordinator 1 is " + coordinator + "with value " + values.getAsString("value"));

						outputStream = getContext().openFileOutput(values.getAsString("key"), Context.MODE_PRIVATE);
						outputStream.write(values.getAsString("value").getBytes());
						outputStream.close();
						if(!keyList.contains(keySet))
							keyList.add(keySet);
					} else if (sucessor2.equals(GlobalContainer.myPort)) {
						Log.e("UPDATING", "in coordinator with key : " + keySet + " the coordinator 2 is " + sucessor2  + "with value " + values.getAsString("value"));

						outputStream = getContext().openFileOutput(values.getAsString("key"), Context.MODE_PRIVATE);
						outputStream.write(values.getAsString("value").getBytes());
						outputStream.close();
						if(!keyList.contains(keySet))
							keyList.add(keySet);

					} else if (sucessor3.equals(GlobalContainer.myPort)) {
						Log.e("UPDATING", "in coordinator with key : " + keySet + " the coordinator 3 is " + sucessor3 + "with value " + values.getAsString("value"));

						outputStream = getContext().openFileOutput(values.getAsString("key"), Context.MODE_PRIVATE);
						outputStream.write(values.getAsString("value").getBytes());
						outputStream.close();
						if(!keyList.contains(keySet))
							keyList.add(keySet);

					}
				} else {

					Log.e("Delete called in update", " deleting..." + selection);
					if (selection.equals("@") || selection.equals("*"))
						return deleteAll();

					if (!keyList.contains(selection)) {
						return 1;
					}

					while (keyList.contains(selection))
						keyList.remove(selection);
					File file = new File(selection);

					if (file.delete())
						return 0;
					else
						return 1;
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		return 0;
	}

    private String genHash(String input) {
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
}
