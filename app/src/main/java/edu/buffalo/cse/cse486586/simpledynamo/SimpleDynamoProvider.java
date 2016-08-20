package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	public static DatabaseHelper dbSQL;
	public static SQLiteDatabase db;
	static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	static String masterPort = "11108";
	static ServerSocket serverSocket;
	static int keyCnt = 0;
	private ServerTask server;
	public static Uri providerUri;
	private String[] cols = {"key", "value"};
	private Cursor dbCursor;
	static String thisDevicePort = "0";
	static String thisDevice = "0";
	public static String predecessor = thisDevicePort;
	public static String successor = thisDevicePort;
	TreeMap<String, String> nodeMap = new TreeMap<String, String>();
	ArrayList<String> nodeList = new ArrayList<String>();
	static boolean setBusyWaiting = false;

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        dbSQL = new DatabaseHelper(getContext(), getTableName(), null, 1);
        db = dbSQL.getWritableDatabase();
        db.execSQL(" DELETE FROM " + getTableName());

        server = new ServerTask();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        thisDevice = portStr;
        thisDevicePort = String.valueOf((Integer.parseInt(portStr) * 2));
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(getContext().getString(R.string.content_uri));
        uriBuilder.scheme("content");
        providerUri = uriBuilder.build();

        try {
            String thisHashedNode = genHash(String.valueOf(Integer.parseInt(thisDevicePort) / 2));
            nodeList.add(thisHashedNode);
            nodeMap.put(thisHashedNode, thisDevicePort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            Log.d("Ex_onCreate","ServerSocket Failed");
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        Log.d("Thisport", thisDevicePort);
        // Request to Join the Network
        if (!thisDevicePort.equals("11108")) {
            new SingleMsgTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "requestjoin");
        }

        return true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key = values.getAsString("key");
        String value = values.getAsString("value");
        try {
            Log.d("KeyNow", key + "->"+genHash(key));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        // TODO Auto-generated method stub
        db.execSQL("DELETE FROM " + getTableName() + " WHERE key='" + key + "'");

        try {

            String hashedKey = genHash(key);
            String myHashedNodeId = genHash(String.valueOf(Integer.parseInt(thisDevicePort) / 2));
            String preHashedNodeId = genHash((Integer.parseInt(predecessor) / 2) + "");

            if ((hashedKey.compareTo(myHashedNodeId) < 0 && hashedKey.compareTo(preHashedNodeId) >= 0) || (successor == "0" && predecessor == "0")) {

                db.insert(getTableName(), null, values);
//                Log.e("Phase1_Insert",nodeMap.toString());
                passToSuccessors("insertToDb",key,value);

                Log.d("InsertVals1", thisDevicePort + "=>" + values.toString());

            } else if (myHashedNodeId.compareTo(preHashedNodeId) < 0 && (hashedKey.compareTo(preHashedNodeId) >= 0 || hashedKey.compareTo(myHashedNodeId) < 0)) {

                Log.d("InsertVals2", thisDevicePort + "=>" + values.toString());
                db.insert(getTableName(), null, values);

                passToSuccessors("insertToDb",key,value);

            } else {
                requestCorrectNode(key, value, "insertnow");
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        Cursor dbCursor = null;


        if (selection.equals("@")) {
            try {
                dbCursor = db.rawQuery("select * from " + getTableName(), null);

            } catch (Exception e) {
                Log.v("query", e.getMessage());
            }
        } else if (selection.equals("*")) {

            MatrixCursor resCursor = new MatrixCursor(cols);

            Cursor tempCursor = db.rawQuery("select * from " + getTableName(), null);
            if (tempCursor != null) {
                while (tempCursor.moveToNext()) {
                    resCursor.addRow(new String[]{tempCursor.getString(tempCursor.getColumnIndex("key")), tempCursor.getString(tempCursor.getColumnIndex("value"))});
//                Log.d("QueryResult", + "=>" + dbCursor.getString(dbCursor.getColumnIndex("value")) + "\n");
                }
            }
            for (int i = 0; i < 5; i++) {

                if (!REMOTE_PORTS[i].equals(thisDevicePort)) {
                    setBusyWaiting = true;
                    SingleMsgTask smTask = (SingleMsgTask) new SingleMsgTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "queryall", REMOTE_PORTS[i]);
                    Integer cnt = 0;
                    while (setBusyWaiting) {
                        //busy wait;
                    }
                    if (smTask.responseObj != null)
                        resCursor = addToCursor(resCursor, (HashMap<String, String>) smTask.responseObj);

                }
            }

            dbCursor = resCursor;

        } else {

            try {

                String hashedKey = genHash(selection);
                String myHashedNodeId = genHash(String.valueOf(Integer.parseInt(thisDevicePort) / 2));
                String preHashedNodeId = genHash((Integer.parseInt(predecessor) / 2) + "");

                if ((hashedKey.compareTo(myHashedNodeId) < 0 && hashedKey.compareTo(preHashedNodeId) >= 0) || (successor == "0" && predecessor == "0")) {
                    Log.d("QUery1", "1");
                    selection = " key='" + selection + "' ";
                    try {
                        dbCursor = db.query(getTableName(), projection, selection, selectionArgs, "", "", sortOrder);
                    } catch (Exception e) {
                        Log.v("query", e.getMessage());
                    }
                } else if (myHashedNodeId.compareTo(preHashedNodeId) < 0 && (hashedKey.compareTo(preHashedNodeId) >= 0 || hashedKey.compareTo(myHashedNodeId) < 0)) {
                    Log.d("QUery2", "2");
                    selection = " key='" + selection + "' ";
                    try {
                        dbCursor = db.query(getTableName(), projection, selection, selectionArgs, "", "", sortOrder);
                    } catch (Exception e) {
                        Log.v("query", e.getMessage());
                    }
                } else {

                    setBusyWaiting = true; // Set to true to get correct response
                    HashMap<String, String> responseHM = (HashMap) requestCorrectNode(selection, "", "querynow");
                    MatrixCursor resCursor = new MatrixCursor(cols);
                    dbCursor = addToCursor(resCursor, responseHM);

                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }

//        while (dbCursor.moveToNext()) {
//            Log.d("Values", dbCursor.getString(dbCursor.getColumnIndex("key")) + "=>" + dbCursor.getString(dbCursor.getColumnIndex("value")) + "\n");
//        }

        return dbCursor;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        int result = 0;

        if (selection.equals("@")) {
            result = db.delete(getTableName(), null, null);
        } else if (selection.equals("*")) {
            result = db.delete(getTableName(), null, null);
            for (int i = 0; i < 5; i++) {
                if (!REMOTE_PORTS[i].equals(thisDevicePort)) {
                    new SingleMsgTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "deleteall", REMOTE_PORTS[i]);
                }
            }
        } else {
            try {

                String hashedKey = genHash(selection);
                String myHashedNodeId = genHash(String.valueOf(Integer.parseInt(thisDevicePort) / 2));
                String preHashedNodeId = genHash((Integer.parseInt(predecessor) / 2) + "");

                if ((hashedKey.compareTo(myHashedNodeId) < 0 && hashedKey.compareTo(preHashedNodeId) >= 0) || (successor == "0" && predecessor == "0")) {
                    Log.d("Del1", "1");
                    String selectionQ = " key='" + selection + "' ";
                    try {
                        result = db.delete(getTableName(), selectionQ, null);
                        passToSuccessors("deletenow",selection,"");
                    } catch (Exception e) {
                        Log.v("query", e.getMessage());
                    }
                } else if (myHashedNodeId.compareTo(preHashedNodeId) < 0 && (hashedKey.compareTo(preHashedNodeId) >= 0 || hashedKey.compareTo(myHashedNodeId) < 0)) {
                    Log.d("Del2", "2");
                    String selectionQ = " key='" + selection + "' ";
                    try {
                        result = db.delete(getTableName(), selectionQ, null);
                        passToSuccessors("deletenow",selection,"");
                    } catch (Exception e) {
                        Log.v("query", e.getMessage());
                    }
                } else {

                    HashMap<String, String> responseHM = (HashMap) requestCorrectNode(selection, "", "deletenow");

                }

            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            String message = "";


            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                Socket clientSocket = serverSocket.accept();
                BufferedReader data = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
                message = data.readLine();
                String response = "Msg response for " + message + "\n";
                String[] msgArr = message.split("__@@__");
                if (msgArr[0].contains("requestjoin")) {

                    try {
                        String newNode = msgArr[1];
                        String hashedNodeId = genHash(String.valueOf(Integer.parseInt(newNode) / 2));
                        nodeMap.put(hashedNodeId, newNode);

                        Log.d("Test_NodeMap", nodeMap.toString());      // DONOT REMOVE Test_ Log statements
                        nodeList.add(hashedNodeId);
                        Collections.sort(nodeList);

                        Log.d("Test_NodeList", nodeList.toString());    // DONOT REMOVE Test_ Log statements

                        if (successor == "0" && predecessor == "0") {
                            successor = newNode;
                            predecessor = newNode;
                        }

                        Integer newNodeIndex = nodeList.indexOf(hashedNodeId);
                        Log.d("Test_nnindex", newNodeIndex+"->"+nodeList.indexOf(hashedNodeId));
                        Integer notifyNode1Index = 0;
                        if (newNodeIndex == 0){
                            notifyNode1Index = nodeList.size() - 1;
                            Log.d("Test_notify1In", notifyNode1Index+"");
                        }
                        else{
                            notifyNode1Index = newNodeIndex - 1;
                            Log.d("Test_notify1InElse", notifyNode1Index+"");
                        }

                        Integer notifyNode2Index = (newNodeIndex + 1) % (nodeList.size());
                        Log.d("Test_notify2In", notifyNode2Index+"");
                        String notifyNode1 = nodeMap.get(nodeList.get(notifyNode1Index));
                        Log.d("Test_notifyNode1", notifyNode1+"");
                        String notifyNode2 = nodeMap.get(nodeList.get(notifyNode2Index));
                        Log.d("Test_notifyNode2", notifyNode2+"");
                        response = notifyNode1 + "_" + notifyNode2 + "\n";
                        Log.d("Test_response", response+"");
                        //Notify this node to update its successor
                        if (notifyNode1.equals(thisDevicePort)) {
                            updatePointers(newNode, "suc");
                            Log.d("Test_EqualIf","1  "+newNode);
                        } else {
                            notifyNode(notifyNode1, newNode, "suc");
                            Log.d("Test_Node1Else", newNode+"  "+notifyNode1);
                        }

                        //Notify this node to update its predecessor
                        if (notifyNode2.equals(thisDevicePort)) {
                            updatePointers(newNode, "pre");
                            Log.d("Test_Equal2If", "1  " + newNode);
                        } else {
                            notifyNode(notifyNode2, newNode, "pre");
                            Log.d("Test_Node2Else", newNode + "  " + notifyNode2);
                        }

                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    respondWithStr(outputStream, response);

                } else if (msgArr[0].contains("update_pointers")) {

                    String[] dataArr = msgArr[1].split("_");
                    Log.d("Test_UpdatePtrs_ST", dataArr[0]+"--"+dataArr[1]);
                    updatePointers(dataArr[0], dataArr[1]);
                    respondWithStr(outputStream, response);

                } else if (msgArr[0].contains("passrequest")) {

                    String[] dataArr = msgArr[1].split("_");
                    ContentValues msgVals = new ContentValues();

                    if (dataArr[0].equals("insert")) {
                        msgVals.put(getContext().getString(R.string.key), dataArr[1]);
                        msgVals.put(getContext().getString(R.string.value), dataArr[2]);
                        getContext().getContentResolver().insert(
                                providerUri,    // assume we already created a Uri object with our provider URI
                                msgVals
                        );
                    }
                    respondWithStr(outputStream, response);
                } else if (msgArr[0].contains("requestlist")) {
                    TreeMap<String, String> responseObj = nodeMap;
                    respondWithObj(outputStream, responseObj);
                } else if (msgArr[0].contains("insertnow")) {

                    String[] dataArr = msgArr[1].split("_");
                    ContentValues msgVals = new ContentValues();

                    msgVals.put(getContext().getString(R.string.key), dataArr[0]);
                    msgVals.put(getContext().getString(R.string.value), dataArr[1]);
                    getContext().getContentResolver().insert(
                            providerUri,    // assume we already created a Uri object with our provider URI
                            msgVals
                    );
                    response = "InsertNow Response:" + dataArr[0] + "->" + dataArr[1];
                    respondWithStr(outputStream, response);
                } else if (msgArr[0].contains("querynow")) {

                    Cursor dbCursor = getContext().getContentResolver().query(providerUri, cols, msgArr[1], null, "");
                    HashMap<String, String> responseObj = new HashMap<String, String>();
                    while (dbCursor.moveToNext()) {
                        responseObj.put(dbCursor.getString(dbCursor.getColumnIndex("key")), dbCursor.getString(dbCursor.getColumnIndex("value")));
                    }
                    respondWithObj(outputStream, responseObj);

                } else if (msgArr[0].contains("deletenow")) {
                    getContext().getContentResolver().delete(providerUri, msgArr[1],null);
                    respondWithStr(outputStream, "Deleted");
                } else if (msgArr[0].contains("insertToDb")) {

                    String[] dataArr = msgArr[1].split("_");
                    ContentValues msgVals = new ContentValues();

                    msgVals.put(getContext().getString(R.string.key), dataArr[0]);
                    msgVals.put(getContext().getString(R.string.value), dataArr[1]);

                    db.insert(getTableName(), null, msgVals);
                    response = "InsertToDb Response:" + dataArr[0] + "->" + dataArr[1];
                    respondWithStr(outputStream, response);

                } else if (msgArr[0].contains("deleteToDb")) {
                    String selectionQ = " key='" + msgArr[1] + "' ";
                    int result = db.delete(getTableName(), selectionQ, null);
                    respondWithStr(outputStream, "Deleted rows "+result);
                }

                publishProgress(message);


            } catch (IOException e) {
                Log.d("Ex_servertask","Server task failed");
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        }
    }

    private class SingleMsgTask extends AsyncTask<String, Void, Void> {

        Object responseObj = null;

        @Override
        protected Void doInBackground(String... params) {

            String sendToPort = null;
            String sendMsg = null;
            if (params[0].equals("requestjoin")) {
                sendToPort = masterPort;
                sendMsg = "requestjoin__@@__" + thisDevicePort + "\n";
                String response = writeString(sendToPort, sendMsg);
                if (!thisDevicePort.equals(masterPort) && response != null) {
                    String[] responseArr = response.split("_");
                    predecessor = responseArr[0];
                    successor = responseArr[1];
                }
            } else if (params[0].equals("update_pointers")) {
                sendToPort = params[1];
                sendMsg = params[0] + "__@@__" + params[2] + "_" + params[3] + "\n";
                writeString(sendToPort, sendMsg);
            } else if (params[0].equals("passrequest")) {
                sendToPort = params[1];
                sendMsg = params[0] + "__@@__" + params[2] + "_" + params[3] + "_" + params[4] + "\n";
                writeString(sendToPort, sendMsg);
            } else if (params[0].equals("requestlist")) {

                sendToPort = params[1];
                sendMsg = params[0] + "__@@__" + params[2] + "\n";
                nodeMap = (TreeMap) writeObject(sendToPort, sendMsg);

                if (params[4] == "insertnow") {
                    String correctNode = findCorrectNodeForKey(nodeMap, params[2]);
                    sendToCorrectNode(params[4], correctNode, params[2], params[3]);
                } else if (params[4] == "querynow") {

                    String correctNode = findCorrectNodeForKey(nodeMap, params[2]);
                    sendToPort = correctNode;
                    sendMsg = params[4] + "__@@__" + params[2] + "\n";
                    HashMap<String, String> resObj = (HashMap) writeObject(sendToPort, sendMsg);
                    this.responseObj = resObj;

                } else if (params[4] == "deletenow") {

                    String correctNode = findCorrectNodeForKey(nodeMap, params[2]);
                    sendToPort = correctNode;
                    sendMsg = params[4] + "__@@__" + params[2] + "\n";
                    HashMap<String, String> resObj = (HashMap) writeObject(sendToPort, sendMsg);

                }
                setBusyWaiting = false;

            } else if (params[0].equals("insertnow") || params[0].equals("insertToDb")) {
                sendToPort = params[1];
                sendMsg = params[0] + "__@@__" + params[2] + "_" + params[3] + "\n";
                writeString(sendToPort, sendMsg);
            } else if (params[0].equals("deletenow") || params[0].equals("deleteFromDb")) {
                sendToPort = params[1];
                sendMsg = params[0] + "__@@__" + params[2] + "\n";
                writeObject(sendToPort, sendMsg);
            } else if (params[0].equals("queryall")) {
                sendToPort = params[1];
                sendMsg = "querynow__@@__@" + "\n";
                HashMap<String, String> resObj = (HashMap) writeObject(sendToPort, sendMsg);
                this.responseObj = resObj;
                setBusyWaiting = false;
            } else if(params[0].equals("deleteall")){
                sendToPort = params[1];
                sendMsg = "deletenow__@@__@" + "\n";
                writeObject(sendToPort, sendMsg);
            }

            return null;
        }
    }

    private String writeString(String sendToPort, String sendMsg) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(sendToPort));

            ObjectOutputStream sendStream = null;
            try {

                sendStream = new ObjectOutputStream(socket.getOutputStream());
                sendStream.write(sendMsg.getBytes());
                sendStream.flush();

//                BufferedReader data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                ObjectInputStream data = new ObjectInputStream(socket.getInputStream());
                String line = null;
                line = data.readUTF();

                socket.close();

                return line;

            } catch (IOException e) {
                Log.d("Exception","INsert failed to "+sendToPort);
//                e.printStackTrace();
            }

        } catch (IOException e) {
            Log.d("Exception", "INsert failed to " + sendToPort);
        }

        return null;
    }

    private Object writeObject(String sendToPort, String sendMsg) {

        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(sendToPort));

            ObjectOutputStream sendStream = null;
            try {
                sendStream = new ObjectOutputStream(socket.getOutputStream());
                sendStream.writeObject(sendMsg);
                sendStream.flush();

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                try {
                    return ois.readObject();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                socket.close();
            } catch (IOException e) {
                Log.d("Ex_writeObject","Failed to"+sendToPort);
            }

        } catch (IOException e) {
            Log.d("Ex_writeObject", "Failed to" + sendToPort);
        }
        return null;
    }

    private void respondWithStr(ObjectOutputStream outputStream, String response) {

        try {
            outputStream.writeUTF(response);
            outputStream.flush();
        } catch (IOException e) {
            Log.d("Ex_respondWithStr", "Response failed ");
        }

    }

    private void respondWithObj(ObjectOutputStream outputStream, Object responseObj) {

        try {
            outputStream.writeObject(responseObj);
            outputStream.flush();
        } catch (IOException e) {
            Log.d("Ex_respondWithObj", "Response failed ");
        }

    }

    private void updatePointers(String newNode, String type) {
        Log.d("updatePointers",newNode+"   "+type);
        if (type.equals("pre"))
            predecessor = newNode;
        if (type.equals("suc"))
            successor = newNode;
    }

    private void notifyNode(String nodePort, String newNodeHashId, String type) {

        // Notify the node to update its Successor/Predecessor
        new SingleMsgTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "update_pointers", nodePort, newNodeHashId, type);

    }

    private Object requestCorrectNode(String key, String value, String typeReq) {

        SingleMsgTask smTask = (SingleMsgTask) new SingleMsgTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "requestlist", masterPort, key, value, typeReq);

        while (setBusyWaiting) {
            // busy wait until correct response set;
        }

        return smTask.responseObj;

    }

    private String findCorrectNodeForKey(TreeMap<String, String> nodeMapNew, String newKey) {

        try {
            String hashedNewKey = genHash(newKey);
            nodeMapNew.put(hashedNewKey, "1");
            Log.d("NodeMapNew", nodeMapNew.toString());
            boolean breakLoop = false;
            for (Map.Entry<String, String> entry : nodeMapNew.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (breakLoop) {
                    nodeMapNew.remove(hashedNewKey);
                    return value;
                }
                if (hashedNewKey.equals(key)) {
                    breakLoop = true;
                }
            }
            nodeMapNew.remove(hashedNewKey);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return nodeMapNew.get(nodeMapNew.firstKey());
    }

    private void sendToCorrectNode(String type, String correctNode, String newKey, String newVal) {
        new SingleMsgTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, type, correctNode, newKey, newVal);
    }

    private MatrixCursor addToCursor(MatrixCursor resCursor, HashMap<String, String> responseHM) {

        Iterator it = responseHM.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
//                        System.out.println(pair.getKey() + " = " + pair.getValue());
            resCursor.addRow(new String[]{(String) pair.getKey(), (String) pair.getValue()});
        }

        return resCursor;
    }

    public String getTableName() {
//        return getContext().getString(R.string.table_name);
        return "messages";
    }

    private String findNextTwoNodes(String thisPortHash) {

        // Replication to Two Consecutive Successors
        setBusyWaiting = true;
        if(nodeMap.size() == 1 && !thisDevicePort.equals(masterPort))
            requestCorrectNode("nokey", "noval", "none"); // Set NodeMap to latest value


        String firstSucc = nodeMap.higherKey(thisPortHash);
        if(firstSucc == null){
            firstSucc = nodeMap.firstKey();
        }

        String secSucc = nodeMap.higherKey(firstSucc);
        if(secSucc == null){
            secSucc = nodeMap.firstKey();
        }

        String firstPort = nodeMap.get(firstSucc);
        String secPort = nodeMap.get(secSucc);

        return firstPort+"_"+secPort;
    }



    private void passToSuccessors(String typeNow, String key, String value) throws NoSuchAlgorithmException {

        String thisPortHash = genHash(String.valueOf(Integer.parseInt(thisDevicePort) / 2));
        String[] nextTwoPorts = findNextTwoNodes(thisPortHash).split("_");
        sendToCorrectNode(typeNow, nextTwoPorts[0], key, value);
        sendToCorrectNode(typeNow, nextTwoPorts[1], key, value);

    }



}
