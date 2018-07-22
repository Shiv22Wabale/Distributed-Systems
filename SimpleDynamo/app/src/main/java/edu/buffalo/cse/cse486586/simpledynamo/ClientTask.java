package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
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
import java.net.SocketTimeoutException;

import edu.buffalo.cse.cse486586.simpledynamo.configurations.GlobalContainer;

/**
 * Created by shivraj on 14/4/18.
 */

public class ClientTask extends AsyncTask< String, Void, Cursor> {

	ContentResolver cr = null;

	public ClientTask( ContentResolver cr ){
		this.cr = cr;
	}

	@Override
	protected Cursor doInBackground(String... strings) {
		try {

			String instruction = strings[0];

			if( instruction.equals("INSERT") ) {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(strings[1]));

				DataOutputStream send = new DataOutputStream(socket.getOutputStream());
				DataInputStream inputStream = new DataInputStream(socket.getInputStream());

				Log.e("CLIENT", "Sending insert : " + strings[1] + " ----------- " + strings[2] + " : " + strings[3] + "  From " + GlobalContainer.myPort);
				send.writeByte(GlobalContainer.INSERT);
				send.writeUTF(strings[2]);
				send.writeUTF(strings[3]);
				send.writeUTF(strings[4]);
				inputStream.read();
			}
			else if(instruction.equals("REQUEST")) {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(strings[1]));

				Log.e("REQUEST", " The client connections sucessful with " + strings[1]);
				DataOutputStream send = new DataOutputStream(socket.getOutputStream());
				DataInputStream rec = new DataInputStream(socket.getInputStream());

				send.writeByte(GlobalContainer.REQUEST);
				send.writeUTF(strings[2]);
				send.writeUTF(strings[3]);

				MatrixCursor matrixCursor = null;

				String[] columnNames = {"key", "value"};
				matrixCursor = new MatrixCursor(columnNames);

				while(rec.readBoolean()) {
					String keySet = rec.readUTF();
					String valueSet = rec.readUTF();
					Log.e("REQUEST", " the return values are  " + keySet + " with value " + valueSet);

					matrixCursor.addRow(new String[]{keySet, valueSet});
				}

				if(matrixCursor.getCount() == 0)
					return matrixCursor;

				return matrixCursor;
			}
			else if(instruction.equals("RECOVERY")) {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(strings[1]));

				Log.e("RECOVERY", " The client connections sucessful with " + strings[1] + " for recovery ");
				DataOutputStream send = new DataOutputStream(socket.getOutputStream());
				DataInputStream rec = new DataInputStream(socket.getInputStream());

				send.writeByte(GlobalContainer.RECOVERY);
				send.writeUTF(strings[2]);

				MatrixCursor matrixCursor = null;

				String[] columnNames = {"key", "value"};
				matrixCursor = new MatrixCursor(columnNames);

				while(rec.readBoolean()) {
					ContentValues cv = new ContentValues();

					String keySet = rec.readUTF();
					String valueSet = rec.readUTF();

					cv.put(GlobalContainer.KEY_FIELD, keySet);
					cv.put(GlobalContainer.VALUE_FIELD, valueSet);

					Log.e("RECOVERY"," trying to recover the key - " + keySet);
					cr.update(GlobalContainer.mUri, cv, "RECOVERY", null);
				}
//				if(matrixCursor.getCount() == 0)
//					return null;
				return matrixCursor;
			}
			else if( instruction.equals("DELETE") ) {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(strings[1]));

				DataOutputStream send = new DataOutputStream(socket.getOutputStream());
				DataInputStream inputStream = new DataInputStream(socket.getInputStream());

				Log.e("CLIENT", "Sending delete : " + strings[1] + " ----------- " + strings[2] + "  From " + GlobalContainer.myPort);
				send.writeByte(GlobalContainer.DELETE);
				send.writeUTF(strings[2]);
			}
		} catch (SocketTimeoutException e){
			Log.e("FAIL", " connection failed 1 " + strings[1]);
			e.printStackTrace();
			return null;
		}
		catch (IOException e) {
			Log.e("FAIL", " connection failed 2 " + strings[1]);
			e.printStackTrace();
			return null;
		}
		catch (Exception e){
			Log.e("FAIL", " connection failed 3 " + strings[1]);
			e.printStackTrace();
			return null;
		}
		return null;
	}
}