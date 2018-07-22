package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    //static final String REMOTE_PORT1 = "11112";
    static final int SERVER_PORT = 10000;

    //Socket[] socket ;

    int msgSeq = 0;

    static final String  AUTHORITY = "edu.buffalo.cse.cse486586.groupmessenger1.provider";
    //public static Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY);

    //static GroupMessengerProvider GROUP_MESSENGER_PROVIDER = new GroupMessengerProvider();

    //Uri.Builder builder = new Uri.Builder();
    private Uri mUri = null;

    Socket[] socket = {null, null, null, null, null};


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);


        setContentView(R.layout.activity_group_messenger);

        Uri.Builder builder = new Uri.Builder();
        builder.authority(AUTHORITY);
        builder.scheme("content");
        mUri = builder.build();


        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e("Server Connection ", "Can't create a ServerSocket");
        }
        //for ( int i = 0; i < 5; i++)
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        



        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
//        String remotePort;





//    };




        final EditText editText = (EditText) findViewById(R.id.editText1);

        editText.setOnKeyListener(new View.OnKeyListener() {
            @Override
            public boolean onKey(View v, int keyCode, KeyEvent event) {
                if ((event.getAction() == KeyEvent.ACTION_DOWN)  &&
                        (keyCode == KeyEvent.KEYCODE_ENTER)) {

                    String msg = editText.getText().toString() + "\n";
                    editText.setText(""); // This is one way to reset the input box.

                    Log.d(TAG, "Key button clicked ------ " + msg);
                    /*
                     * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                     * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                     * the difference, please take a look at
                     * http://developer.android.com/reference/android/os/AsyncTask.html
                     */
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

                    return true;
                }
                return false;
            }
        });

        final Button send = (Button) findViewById(R.id.button4);



        send.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                Log.d(TAG, "Send button clicked ------ " + msg);
                //TextView localTextView = (TextView) findViewById(R.id.local_text_display);
                //localTextView.append("\t" + msg); // This is one way to display a string.
                //TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
                //remoteTextView.append("\n");

                    /*
                     * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                     * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                     * the difference, please take a look at
                     * http://developer.android.com/reference/android/os/AsyncTask.html
                     */
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);

                //return true;
            }
            //return false;
        });
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket[] serverSocket = {sockets[0], sockets[0], sockets[0], sockets[0], sockets[0]};

            try {
                /*
                 * TODO: Fill in your server code that receives messages and passes them
                 * to onProgressUpdate().
                 */

                //DataInputStream[] rec = {null, null, null, null, null};

                DataInputStream rec;
                Socket[] receiver = {null, null, null, null, null};

                for (int i = 0; i < 5; i++) {
                    receiver[i] = serverSocket[i].accept();
                    Log.e("Created the server " + i,"Trying to read the msg........" + serverSocket[i]);
                }

                Log.e("Waiting","To receive $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
                String receivedMSG;

                InputStream[] inputStream = {receiver[0].getInputStream(), receiver[1].getInputStream(),
                        receiver[2].getInputStream(), receiver[3].getInputStream(), receiver[4].getInputStream()};

                int i = 0;
                byte[] buffer = new byte[1024];
                while(true) {

                    //for(i = 0; i < 2; i ++) {
                        if(inputStream[i].available() != 0) {
                            rec = new DataInputStream(receiver[i].getInputStream());


                            //inputStream[i].read(buffer);
                            receivedMSG = rec.readUTF();

                            //receivedMSG = buffer.toString();

                            Log.d("Success", receivedMSG);

                            publishProgress(receivedMSG);

                            //rec = null;
                        }
                        TimeUnit.MILLISECONDS.sleep(10);
                        i = (i + 1) % 5;
                    //}
                    //onProgressUpdate(rec.readUTF());
                }
                //receiver.close();
                //serverSocket = new ServerSocket(SERVER_PORT);
                //new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException"+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException "+e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return null;
        }

        @Override
        protected void onPostExecute(Void aVoid) {
            super.onPostExecute(aVoid);
            Log.d(TAG, "onPostExecute: Stoping AsyncTask");
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            //Log.d("Success in Progress", strReceived);

            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");

            ContentValues contentValues = new ContentValues();

            contentValues.put("key", Integer.toString(msgSeq));
            contentValues.put("value", strReceived);
            msgSeq++;

            //Uri uri = new

            getContentResolver().insert(mUri, contentValues);
            //GROUP_MESSENGER_PROVIDER.insert(CONTENT_URI,contentValues);

            //    }
            //});
            return;
        }



    }


    private class SetClientSocket extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            for (int i = 0; i < 5; i++) {
                try {
                    socket[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[i]));
                    //Log.d(TAG, "Sockets are created....!!!!!!" + " Socket no . " + i);
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.d(TAG, "Errorsssssssss.................!!!!!!!!!!!!!!!!");
                }
           }

            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                //if (msgs[1].equals(REMOTE_PORT0))
                //    remotePort = REMOTE_PORT1;
                Log.d(TAG, "inside thread   " + msgs[0]);

                //Iterator iterator = REMOTE_PORTS.
                for(int i = 0; i < 5; i++) {
                    //Log.d(TAG, "inside for loop   " + i);
                    //if(remotePort.compareTo(msgs[1]) != 0) {
                    Socket socket_current = socket[i];

                        String msgToSend = msgs[0];

                        //Log.d(TAG, "Sending msg to ------ " + socket[i] + " msg is ----- " + msgToSend);

                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                        DataOutputStream send = new DataOutputStream(socket_current.getOutputStream());

                        send.writeUTF(msgToSend);


                    //}
                }
                //socket.close();
            } catch (UnknownHostException e) {
                //Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                //Log.e(TAG, "ClientTask socket IOException");
            }
            return null;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);


        return true;
    }

    @Override
    protected void onStart() {
        super.onStart();

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new SetClientSocket().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    //oncreate
}
