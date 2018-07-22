package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.Html;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static String[] source = {"", "", "", "", ""};

    static AtomicBoolean failedServer = new AtomicBoolean(false);
    static String faultServer = "";

    static final int SERVER_PORT = 10000;

    static final byte MESSAGE = 0;
    static final byte REMESSAGE = 1;
    static final byte SEQUENCE = 2;
    static final byte CONFIRMATION = 3;

    static HashMap<String, Integer> messageMap = new HashMap<String, Integer>();
    static HashMap<String, Integer> ackCount = new HashMap<String, Integer>();
    //static int highestSeq = 0;
    static AtomicInteger highestSeq = new AtomicInteger(0);
    //static HashMap<String, Integer> messsageQueue = new HashMap<String, Integer>();
    static ConcurrentHashMap<String, Integer> messsageQueue = new ConcurrentHashMap<String, Integer>();
    //static HashMap<String, String> messsageSource = new HashMap<String, String>();
    static ConcurrentHashMap<String, String> messsageSource = new ConcurrentHashMap<String, String>();

    static CopyOnWriteArrayList<String> readytoCommit = new CopyOnWriteArrayList<String>();

    static AtomicBoolean setClientSocket = new AtomicBoolean(false);


    static int commitSeq = 0;

    static String myPort;

    static final String  AUTHORITY = "edu.buffalo.cse.cse486586.groupmessenger2.provider";
    private Uri mUri = null;

    static AtomicInteger expectedResponse = new AtomicInteger(0);

    Socket[] socket = {null, null, null, null, null};
    InputStream[] inputStreamClient = {null, null, null, null, null};

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e("Server Connection ", "Can't create a ServerSocket");
        }

        //for ( int i = 0; i < 5; i++)
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "SetClient");

//        try {
//            TimeUnit.SECONDS.sleep(5);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        setContentView(R.layout.activity_group_messenger);


        Uri.Builder builder = new Uri.Builder();
        builder.authority(AUTHORITY);
        builder.scheme("content");
        mUri = builder.build();
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

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button send = (Button) findViewById(R.id.button4);

        send.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {


                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
            //    Log.d(TAG, "Send button clicked ------ " + msg);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

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

                DataInputStream rec;
                DataOutputStream out;
                Socket[] receiver = {null, null, null, null, null};

                for (int i = 0; i < 5; i++) {
                    receiver[i] = serverSocket[i].accept();
                    source[i] = ( new DataInputStream(receiver[i].getInputStream()) ).readUTF();

                    Log.e("Created the server " + i, " with source as " + source[i]);
                //    Log.e("Created the server " + i,"Trying to read the msg........" + serverSocket[i].getInetAddress().getHostAddress() );
                }

                for(int i = 0; i < 5; i++)
                    serverSocket[i].setSoTimeout(500);

                Log.e("Waiting","To receive $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
                publishProgress("$$$$$");

                String receivedMSG = "NOT RECEIVE MSG";
                byte type;

                InputStream[] inputStream = {receiver[0].getInputStream(), receiver[1].getInputStream(),
                        receiver[2].getInputStream(), receiver[3].getInputStream(), receiver[4].getInputStream()};

                OutputStream[] outputStreams = {receiver[0].getOutputStream(), receiver[1].getOutputStream(),
                        receiver[2].getOutputStream(), receiver[3].getOutputStream(), receiver[4].getOutputStream()};

                int i = 0;
                //byte[] buffer = new byte[1024];
                while(true) {

                    //for(i = 0; i < 5; i ++) {
                    try {
                        //if(source[i].compareTo(failedServer) == 0)
                        //    continue;

//                        if(!receiver[i].getInetAddress().isReachable(500)) {
//                            Log.e("Destination is not ", " Exception raised...");
//                            throw new SocketTimeoutException();
//                        }

                        if(source[i].compareTo(faultServer) == 0)
                            i = (i + 1) % 5;

                        if(inputStream[i].available() != 0) {
                            rec = new DataInputStream(inputStream[i]);
                            type = rec.readByte();
                            switch (type) {
                                case MESSAGE:
                                    receivedMSG = rec.readUTF();

                                    publishProgress(source[i]);
                                    out = new DataOutputStream(outputStreams[i]);

                                    out.writeUTF(Integer.toString(highestSeq.get() + 1));
                                    //out.writeUTF(receivedMSG);

                                    messsageSource.put(receivedMSG, rec.readUTF());

                                    messsageQueue.put( receivedMSG, (highestSeq.getAndAdd(1)) * 100000 + Integer.parseInt(myPort) );
                                    //highestSeq += 1;
                                    //highestSeq.add
                                    Log.d(myPort + "Message received ", receivedMSG);


                                    type = rec.readByte();

                                    if(type == CONFIRMATION) {

                                        receivedMSG = rec.readUTF();
                                        int finalSeq = Integer.parseInt(rec.readUTF());
                                        int port = Integer.parseInt(rec.readUTF());
                                        highestSeq.set(highestSeq.get() > finalSeq ? highestSeq.get() : finalSeq);
                                        messsageQueue.put(receivedMSG, finalSeq * 100000 + port);
                                        readytoCommit.add(receivedMSG);
                                        //messsageSource.put(receivedMSG, port);

                                        //    Log.d(TAG, "doInBackground: " + finalSeq + " for Message : " + receivedMSG);

                                        //if(finalSeq == Collections.min(messageMap.values()) )
                                        //if (canCommit(Collections.min(messsageQueue.values())))
                                            publishProgress();

                                    }

                                    break;

                                case CONFIRMATION:
                                    receivedMSG = rec.readUTF();
                                    int finalSeq = Integer.parseInt(rec.readUTF());
                                    int port = Integer.parseInt(rec.readUTF());
                                    highestSeq.set(highestSeq.get() > finalSeq ? highestSeq.get() : finalSeq);
                                    messsageQueue.put(receivedMSG, finalSeq * 100000 + port);
                                    readytoCommit.add(receivedMSG);
                                    //messsageSource.put(receivedMSG, port);

                                    //    Log.d(TAG, "doInBackground: " + finalSeq + " for Message : " + receivedMSG);

                                    //if(finalSeq == Collections.min(messageMap.values()) )
                                    //if (canCommit(Collections.min(messsageQueue.values())))
                                        publishProgress();

                                    break;

                                default:
                                    break;
                            }
                        }

                    }
                    catch(SocketTimeoutException e){
                        if(!failedServer.get()) {
                            failedServer.set(true);
                            faultServer = source[i];
                            publishProgress("NodeFail", source[i]);
                            //failedServer = source[i];
                            //removeDeadMessages(source[i]);
                            publishProgress("RemoveDeadMessages", source[i]);

                        }
                    }
                    catch (IOException e){
                        if(!failedServer.get()) {
                            failedServer.set(true);
                            faultServer = source[i];
                            publishProgress("NodeFail", source[i]);
                            //failedServer = source[i];
                            //removeDeadMessages(source[i]);
                            publishProgress("RemoveDeadMessages", source[i]);

                        }
                    }
                    catch (Exception e){
                        publishProgress("Exception", e.getMessage());
                    }

                    TimeUnit.MILLISECONDS.sleep(5);
                    i = (i + 1) % 5;
                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
                Log.e(TAG, "ServerTask UnknownHostException"+e.getMessage());
                publishProgress("Exception", e.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ServerTask socket IOException "+e.getMessage());
                publishProgress("Exception", e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
                publishProgress("Exception", e.getMessage());
            } catch (Exception e){
                publishProgress("Exception", e.getMessage());
            }


            return null;
        }

        void removeDeadMessages(String sourcePort) {

            if( messsageSource.isEmpty() )
                return;

            for(String msg: messsageSource.keySet()) {
                if(messsageSource.get(msg).compareTo(sourcePort) == 0) {
                    if( messsageQueue.isEmpty() )
                        return;
                    for(String receivedMSG : messsageQueue.keySet()) {
                        if(receivedMSG.compareTo(msg) == 0 ){
                            messsageQueue.remove(receivedMSG);
                            if(readytoCommit.contains(receivedMSG))
                                readytoCommit.remove(receivedMSG);
                            //if (canCommit(Collections.min(messsageQueue.values())))
                            //publishProgress();
                            commitMessages();
                        }
                    }
                }
            }
        }

        boolean canCommit(int min) {
            for(String msg : messsageQueue.keySet()) {
                //Log.e("in getLowestMsg", msg);
                if (messsageQueue.get(msg) == min && readytoCommit.contains(msg)) {
                    Log.e("Next message to commit", "Is " + msg + " with confirmation as  " + min);
                    return true;
                }
            }
            return false;
        }

        String getLowestMsg(int min) {

            for(String msg : messsageQueue.keySet()) {
                //Log.e("in Messages in queue", msg);
                if (messsageQueue.get(msg) == min && readytoCommit.contains(msg))
                    return msg;
            }
            return null;
        }

        void printQueue() {
            Log.d("Starting",">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            Log.e("The min is ", "   " + Collections.min(messsageQueue.values()));
            for(String msg : messsageQueue.keySet()) {
                if(readytoCommit.contains(msg))
                    Log.e("message is " + msg, "with key as " + messsageQueue.get(msg));
            }
            Log.d("Ending",">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }

        protected void onProgressUpdate(String... strings) {

            //List<String> listMessages = null;

            if(strings.length > 0){
                //if()
                if(strings[0].compareTo("NodeFail") == 0) {
                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append(Html.fromHtml("<font color='#EE0000'>Detected node failure  :  " + strings[1] + " detected by Server thread..... </font>\n" ) );
                }
                else if(strings[0].compareTo("$$$$$") == 0){
                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append("$$$$$$$$$$$$$$$$$$$$$$\n");
                }
                else if(strings[0].compareTo("RemoveDeadMessages") == 0){
                    removeDeadMessages(strings[1]);
                }
                else if(strings[0].compareTo("Exception") == 0){
                    //removeDeadMessages(strings[1]);
                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append(Html.fromHtml("<font color='#EE0000'>Exception:  " + strings[1] + "</font>\n" ) );
                }
                else {
                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append("Msg rec from : " + strings[0] + "\n");
                }
                return;
            }

            commitMessages();

            return;
        }


        protected void commitMessages(){
            if(messsageQueue.isEmpty())
                return;
            if (!canCommit(Collections.min(messsageQueue.values())))
                return;

            //    Log.d("Message Queue", "Is empty" + messsageQueue.isEmpty());
            if(messsageQueue.isEmpty())
                return;
            String strReceived = getLowestMsg(Collections.min(messsageQueue.values()));

            do {
                if(strReceived == null)
                    return;
                //printQueue();


                String selection = Integer.toString(commitSeq++);
                //String selection = strings[1];

                //        Log.e("Success in Progress ", "Committing the seq    " + strReceived + " with seq    " + selection);

                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append(selection + " : " + strReceived + "\t\n");




                //if (getContentResolver().query(mUri, null, selection, null, null) == null) {


                ContentValues contentValues = new ContentValues();

                contentValues.put("key", selection);
                contentValues.put("value", strReceived.split(",")[0]);

                getContentResolver().insert(mUri, contentValues);

                messsageQueue.remove(strReceived);
                readytoCommit.remove(strReceived);


                //}
                if(!readytoCommit.isEmpty() && !messsageQueue.isEmpty()){
                    //listMessages = getKeyMessage();
                    if(canCommit(Collections.min(messsageQueue.values())))
                        strReceived = getLowestMsg(Collections.min(messsageQueue.values()));
                    else
                        break;
                    //break;
                    //continue;
                }
                else
                    break;
            }
            while( true );
        }

    }

    static AtomicInteger sendingSeq = new AtomicInteger(0);

    private class ClientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                if(!setClientSocket.get() || msgs[0].compareTo("SetClient") == 0) {
                    setClientSocket.set(true);
                    TimeUnit.SECONDS.sleep(8); // The delay required to make sure that all the apks are running
                    for (int i = 0; i < 5; i++) {
                        try {
                            //new Socket()
                            socket[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[i]));
                            //socket[i].connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[i])), 50000 );
                            inputStreamClient[i] = socket[i].getInputStream();
//                        sockets[2].getInputStream(), sockets[3].getInputStream(), sockets[4].getInputStream()};
                            Log.d(TAG, "ClientSockets are created....!!!!!!" + " Socket no . " + i);
                            new DataOutputStream(socket[i].getOutputStream()).writeUTF(myPort);
                        } catch (IOException e) {
                            e.printStackTrace();
                            Log.d(TAG, "Errorsssssssss.................!!!!!!!!!!!!!!!!");
                        }
                    }

                    for(int i = 0; i < 5; i++) {
                        socket[i].setSoTimeout(50000);
                    }


                    if(msgs[0].compareTo("SetClient") == 0)
                        return null;
                    //new ResendClient().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket );
                }

               Log.d(TAG, "trying to send   " + msgs[0]);

                String msgToSend = msgs[0] + "," + myPort + sendingSeq.get();
                //ackCount.put(msgToSend, 0);

                //messageMap.put(msgToSend, highestSeq.get());

                int seqRec = 0;

                for(int i = 0; i < 5; i++) {
                    try {
                        //Log.d(TAG, "trying to send   " + msgs[0] + " to --------    " + REMOTE_PORTS[i]);
                        //if(REMOTE_PORTS[i].compareTo(failedServer) == 0)
                        //    continue;
                        Socket socket_current = socket[i];
                        //socket_current.(500);

                        DataOutputStream send = new DataOutputStream(socket_current.getOutputStream());

                        send.writeByte(MESSAGE);
                        send.writeUTF(msgToSend);
                        send.writeUTF(myPort);

                        //messageMap.put(msgToSend, highestSeq.get());


                        DataInputStream rec = new DataInputStream(inputStreamClient[i]);

                        seqRec = Math.max(seqRec, Integer.parseInt(rec.readUTF()));

                        //String keyMSG = rec.readUTF();
                        //int seq = messageMap.get(keyMSG);

                        //seq = ( seqRec > seq ? seqRec : seq);
                        //messageMap.put(keyMSG, seq);

                        //Log.d(TAG, "Sent   " + msgs[0] + " to  " + REMOTE_PORTS[i] + " DONE");

                    }
                    catch (SocketTimeoutException e){
                        if(!failedServer.get()) {
                            failedServer.set(true);
                            faultServer = REMOTE_PORTS[i];
                            publishProgress("NodeFail", REMOTE_PORTS[i]);
                            //failedServer = REMOTE_PORTS[i];
                            //removeDeadMessages(REMOTE_PORTS[i]);
                            publishProgress("RemoveDeadMessages", REMOTE_PORTS[i]);
                        }
                    }
                    catch (IOException e) {
                        if(!failedServer.get()) {
                            failedServer.set(true);
                            faultServer = REMOTE_PORTS[i];
                            publishProgress("NodeFail", REMOTE_PORTS[i]);
                            //failedServer = REMOTE_PORTS[i];
                            //removeDeadMessages(REMOTE_PORTS[i]);
                            publishProgress("RemoveDeadMessages", REMOTE_PORTS[i]);
                        }
                    }
                    catch (Exception e){
                        publishProgress("Exception", e.getMessage());
                    }
                }

                for(int i = 0; i < 5; i++) {
                    try {
                        Socket socket_current = socket[i];

                        DataOutputStream send = new DataOutputStream(socket_current.getOutputStream());

                        send.writeByte(CONFIRMATION);
                        send.writeUTF(msgToSend);
                        send.writeUTF( Integer.toString( seqRec ) );
                        send.writeUTF(myPort);
                    }
                    catch (SocketTimeoutException e){
                        if(!failedServer.get()) {
                            failedServer.set(true);
                            faultServer = REMOTE_PORTS[i];
                            publishProgress("NodeFail", REMOTE_PORTS[i]);
                            //failedServer = REMOTE_PORTS[i];
                            //removeDeadMessages(REMOTE_PORTS[i]);
                            publishProgress("RemoveDeadMessages", REMOTE_PORTS[i]);
                        }
                    }
                    catch (Exception e){
                        publishProgress("Exception", e.getMessage());
                    }
                }


                sendingSeq.getAndAdd(1);
                expectedResponse.addAndGet(1);

            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "ClientTask socket IOException");
                publishProgress("Exception", e.getMessage());
            } catch (InterruptedException e) {
                e.printStackTrace();
                publishProgress("Exception", e.getMessage());
            } catch (Exception e){
                publishProgress("Exception", e.getMessage());
            }

            return null;
        }


        void removeDeadMessages(String sourcePort) {
            if( messsageSource.isEmpty() )
                return;
            for(String msg: messsageSource.keySet()) {
                if(messsageSource.get(msg).compareTo(sourcePort) == 0) {
                    if( messsageQueue.isEmpty() )
                        return;
                    for(String receivedMSG : messsageQueue.keySet()) {
                        if(receivedMSG.compareTo(msg) == 0){
                            messsageQueue.remove(receivedMSG);
                            if(readytoCommit.contains(receivedMSG))
                                readytoCommit.remove(receivedMSG);
                            //if (canCommit(Collections.min(messsageQueue.values())))
                            //publishProgress();
                            commitMessages();
                        }
                    }
                }
            }
        }

        boolean canCommit(int min) {
            for(String msg : messsageQueue.keySet()) {
                //Log.e("in getLowestMsg", msg);
                if (messsageQueue.get(msg) == min && readytoCommit.contains(msg)) {
                    Log.e("Next message to commit", "Is " + msg + " with confirmation as  " + min);
                    return true;
                }
            }
            return false;
        }

        String getLowestMsg(int min){

            for(String msg : messsageQueue.keySet()) {
                //Log.e("in Messages in queue", msg);
                if (messsageQueue.get(msg) == min && readytoCommit.contains(msg))
                    return msg;
            }
            return null;
        }

        void printQueue(){
            Log.d("Starting",">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            Log.e("The min is ", "   " + Collections.min(messsageQueue.values()));
            for(String msg : messsageQueue.keySet()) {
                if(readytoCommit.contains(msg))
                    Log.e("message is " + msg, "with key as " + messsageQueue.get(msg));
            }
            Log.d("Ending",">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        }


        protected void onProgressUpdate(String... strings) {

            //List<String> listMessages = null;

            if(strings.length > 0){


                if(strings[0].compareTo("NodeFail") == 0) {
                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append(Html.fromHtml("<font color='#EE0000'>Detected node failure  :  " + strings[1] + " detected by Client thread..... </font>\n" ) );
                }
                else if(strings[0].compareTo("RemoveDeadMessages") == 0){
                    removeDeadMessages(strings[1]);
                }
                else if(strings[0].compareTo("Exception") == 0){
                    //removeDeadMessages(strings[1]);
                    TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                    remoteTextView.append(Html.fromHtml("<font color='#EE0000'>Exception:  " + strings[1] + "</font>\n" ) );
                }

                return;

            }

            if (!canCommit(Collections.min(messsageQueue.values())))
                return;

            commitMessages();
        }

        protected void commitMessages(){
            if (!canCommit(Collections.min(messsageQueue.values())))
                return;

            //    Log.d("Message Queue", "Is empty" + messsageQueue.isEmpty());
            if(messsageQueue.isEmpty())
                return;
            String strReceived = getLowestMsg(Collections.min(messsageQueue.values()));

            do {
                if(strReceived == null)
                    return;
                //printQueue();


                String selection = Integer.toString(commitSeq++);
                //String selection = strings[1];

                //        Log.e("Success in Progress ", "Committing the seq    " + strReceived + " with seq    " + selection);

                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append(selection + " : " + strReceived + "\t\n");




                //if (getContentResolver().query(mUri, null, selection, null, null) == null) {


                ContentValues contentValues = new ContentValues();

                contentValues.put("key", selection);
                contentValues.put("value", strReceived.split(",")[0]);

                getContentResolver().insert(mUri, contentValues);

                messsageQueue.remove(strReceived);
                readytoCommit.remove(strReceived);


                //}
                if(!readytoCommit.isEmpty() && !messsageQueue.isEmpty()){
                    //listMessages = getKeyMessage();
                    if(canCommit(Collections.min(messsageQueue.values())))
                        strReceived = getLowestMsg(Collections.min(messsageQueue.values()));
                    else
                        break;
                    //break;
                    //continue;
                }
                else
                    break;
            }
            while( true );
        }

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}