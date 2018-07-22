package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

/**
 * Created by shivraj on 23/3/18.
 */

class ClientTask extends AsyncTask<String, String, Cursor> {

	static final String TAG = ServerTask.class.getSimpleName();
	static final String notifyTo = "11108";

	HashMap<String, String> msgSet = null;
	public ClientTask(HashMap<String, String> msgSet){
		this.msgSet = msgSet;
	}

	@Override
	protected Cursor doInBackground(String... strings) {

		try {

			if(strings[0].equals("NOTICE")) {

				if (strings[1].equals("notify")) {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(notifyTo));
					DataOutputStream send = new DataOutputStream(socket.getOutputStream());

					Log.e(TAG, "Sending msgs : " + strings[1] + " ----------- " + strings[2] + "  From " + SimpleDhtActivity.myPort);
					send.writeByte(GlobalContainer.NOTICE);
					send.writeUTF(strings[1]);
					send.writeUTF(strings[2]);
				}
				else {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(strings[3]));
					DataOutputStream send = new DataOutputStream(socket.getOutputStream());

					Log.e(TAG, "Sending msgs : " + strings[1] + " ----------- to " + strings[3] + "  Set predecessor as " + strings[2]);
					send.writeByte(GlobalContainer.NOTICE);
					send.writeUTF(strings[1]);
					send.writeUTF(strings[2]);
				}
			}
			else if(strings[0].equals("INSERT")) {

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(GlobalContainer.predecessor));
				DataOutputStream send = new DataOutputStream(socket.getOutputStream());

				DataInputStream rec = new DataInputStream(socket.getInputStream());

				send.writeByte(GlobalContainer.INSERT);
				send.writeUTF(strings[1]);
				send.writeUTF(strings[2]);
//				return rec.readUTF();
			}
			else if(strings[0].equals("REQUEST")) {

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(GlobalContainer.predecessor));

				Log.e("REQUEST", " The client connections sucessful with " + GlobalContainer.predecessor);
				DataOutputStream send = new DataOutputStream(socket.getOutputStream());

				DataInputStream rec = new DataInputStream(socket.getInputStream());

				send.writeByte(GlobalContainer.REQUEST);

//				if(strings[1].equals("*@"))
//					send.writeUTF(strings[1]);
//				else
//					send.writeUTF(SimpleDhtActivity.myPort);

				send.writeUTF(strings[1]);
				send.writeUTF(strings[2]);

				//Boolean end = rec.readBoolean();
				Cursor cursor = null;

				MatrixCursor matrixCursor = null;

				String[] columnNames = {"key", "value"};
				matrixCursor = new MatrixCursor(columnNames);

				while(rec.readBoolean()) {
					String keySet = rec.readUTF();
					String valueSet = rec.readUTF();

					matrixCursor.addRow(new String[]{keySet, valueSet});

				}

				if(matrixCursor.getCount() == 0)
					return null;

				return matrixCursor;
				//return rec.readUTF();
			}
			else if(strings[0].equals("SHUFFLE")) {

				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(strings[1]));
				DataOutputStream send = new DataOutputStream(socket.getOutputStream());

				send.writeByte(GlobalContainer.SHUFFLE);
				send.writeInt(msgSet.size());
				for(String key : msgSet.keySet()) {
					send.writeUTF(key);
					send.writeUTF(msgSet.get(key));
				}
				//			send.writeUTF(strings[1]);
				return null;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}


		return null;
	}



}