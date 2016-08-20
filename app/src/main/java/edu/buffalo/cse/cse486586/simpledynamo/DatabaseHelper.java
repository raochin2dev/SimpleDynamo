package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by civa on 2/10/16.
 */
public class DatabaseHelper extends SQLiteOpenHelper {


    private static int DATABASE_VERSION = 2;
    private static String TABLE_NAME =  "";
    private static String TABLE_CREATE = "";
    private static Context appContext;
    SQLiteDatabase db;

    DatabaseHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
        super(context, name, null, version);
        TABLE_NAME = name;
        appContext = context;

        db = getWritableDatabase();
        if(db != null)
            db.execSQL("DELETE FROM '" + TABLE_NAME + "'");
    }


    @Override
    public void onCreate(SQLiteDatabase db) {


        TABLE_CREATE =
        "CREATE TABLE " + TABLE_NAME + " (" +
                appContext.getString(R.string.key) + " TEXT, " +
                appContext.getString(R.string.value) + " TEXT," +
                "PRIMARY KEY ("+ appContext.getString(R.string.key) +") )" +
                ";" ;
        db.execSQL(TABLE_CREATE);

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    }
}
