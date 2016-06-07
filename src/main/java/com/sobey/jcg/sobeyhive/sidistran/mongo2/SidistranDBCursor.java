package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.List;

import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

/**
 * Created by WX on 2016/4/7.
 */
public class SidistranDBCursor extends DBCursor {

    private Cursor __cursor;

    SidistranDBCursor(DBCollection collection, DBObject query,
                             DBObject fields, ReadPreference readPreference) {
        super(collection, query, fields, readPreference);
    }

    SidistranDBCursor(DBCollection collection, Cursor cursor) {
        super(collection, null, null, collection.getReadPreference());
        this.__cursor = cursor;
    }

    @Override
    public long getCursorId() {
        if(__cursor!=null){
            return __cursor.getCursorId();
        }else{
            return super.getCursorId();
        }
    }

    @Override
    public ServerAddress getServerAddress() {
        if(__cursor!=null){
            return __cursor.getServerAddress();
        }else{
            return super.getServerAddress();
        }
    }

    @Override
    public void close() {
        if(__cursor!=null){
            __cursor.close();
        }else{
            super.close();
        }
    }

    @Override
    public boolean hasNext() {
        if(__cursor!=null){
            return __cursor.hasNext();
        }else{
            return super.hasNext();
        }
    }

    @Override
    public void remove() {
        if(__cursor!=null){
            __cursor.remove();
        }else{
            super.remove();
        }
    }

    @Override
    public DBObject next() {
        DBObject dbObject;
        if(__cursor!=null){
            dbObject = __cursor.next();
        }else {
            dbObject = super.next();
        }
        ((SidistranDBCollection)getCollection()).updateResult(dbObject);
        return dbObject;
    }

    @Override
    public DBObject one() {
        DBObject dbObject = super.one();
        ((SidistranDBCollection)getCollection()).updateResult(dbObject);
        return dbObject;
    }

    @Override
    public DBObject tryNext() {
        DBObject dbObject =  super.tryNext();
        ((SidistranDBCollection)getCollection()).updateResult(dbObject);
        return dbObject;
    }

    @Override
    public List<DBObject> toArray() {
        List<DBObject> list = super.toArray();
        if(list!=null&&!list.isEmpty()){
            for(DBObject dbObject: list){
                ((SidistranDBCollection)getCollection()).updateResult(dbObject);
            }
        }
        return list;
    }

    @Override
    public DBObject curr() {
        DBObject dbObject = super.curr();
        ((SidistranDBCollection)getCollection()).updateResult(dbObject);
        return dbObject;
    }

    @Override
    public List<DBObject> toArray(int max) {
        List<DBObject> list = super.toArray(max);
        if(list!=null&&!list.isEmpty()){
            for(DBObject dbObject: list){
                ((SidistranDBCollection)getCollection()).updateResult(dbObject);
            }
        }
        return list;
    }

    @Override
    public DBCursor copy() {
        return new SidistranDBCursor(this.getCollection(), this.getQuery(), this.getKeysWanted(), this.getReadPreference());
    }
}
