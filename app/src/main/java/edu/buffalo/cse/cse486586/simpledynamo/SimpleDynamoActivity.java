package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.database.Cursor;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;
import android.widget.Toast;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class SimpleDynamoActivity extends Activity {

    static SimpleDynamoProvider dhtProvider;
    static String p = null;
    static String s = null;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

//		findViewById(R.id.button3).setOnClickListener(
//				new OnTestClickListener(tv, getContentResolver()));

		dhtProvider = new SimpleDynamoProvider();
        final String key = "t0bKANy6OBu4VSXUqfVebYAkRxg623p7";
//		final String key = "lG68PUGaOyyCzAKZLSEtnxQLrtOnMcXQ";
		findViewById(R.id.button4).setOnClickListener(
				new View.OnClickListener() {
					public void onClick(View v) {
						// Perform action on click
						if (p == null && s == null) {
							p = dhtProvider.predecessor;
							s = dhtProvider.successor;
							Log.d("SDAct_P/S Initial", p + "/" + s);
						} else {
							Log.d("SDAct_P/S Now", dhtProvider.predecessor + "/" + dhtProvider.successor);
						}
						ContentValues msgVals = new ContentValues();
						Toast.makeText(getApplicationContext(), "SDAct_P/S:" + dhtProvider.predecessor + "/" + dhtProvider.successor,
								Toast.LENGTH_LONG).show();

                        try {
                            Log.d("SDAct_InsertValues", genHash(key));
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }

                        msgVals.put(getApplicationContext().getString(R.string.key), key);
                        msgVals.put(getApplicationContext().getString(R.string.value), "bWdNN5H8YAM6jFbi0sJr9TL4qLa8dxYg");
                        getApplicationContext().getContentResolver().insert(
                                dhtProvider.providerUri,    // assume we already created a Uri object with our provider URI
                                msgVals
                        );

					}
				});

		findViewById(R.id.button5).setOnClickListener(
				new View.OnClickListener() {
					public void onClick(View v) {
						// Perform action on click
						String[] cols = new String[]{"key", "value"};
						Cursor dbCursor = getApplicationContext().getContentResolver().query(dhtProvider.providerUri, cols, "*", null, "");
						if (dbCursor != null) {
							while (dbCursor.moveToNext()) {
								Log.d("QueryResult", dbCursor.getString(dbCursor.getColumnIndex("key")) + "=>" + dbCursor.getString(dbCursor.getColumnIndex("value")) + "\n");
							}
						}
					}
				});

		findViewById(R.id.button6).setOnClickListener(
				new View.OnClickListener() {
					public void onClick(View v) {
						// Perform action on click
//                        String where = key;
						String where = "*";
						int result = getApplicationContext().getContentResolver().delete(dhtProvider.providerUri, where, null);
						Log.d("DelResult", String.valueOf(result));
					}
				});

	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
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
