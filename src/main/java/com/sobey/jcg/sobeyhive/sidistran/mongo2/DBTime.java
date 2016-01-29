package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * Created by WX on 2016/1/26.
 */
class DBTime {
    private static ConcurrentMap<String, DBTime> map = new ConcurrentHashMap();
    private DBCollection clt;

    private static String ID_GEN = "DB_TIME";
    private static String SEQ_FIELD = "time";

    @SuppressWarnings("deprecation")
    private DBTime(MongoClient mongoClient) {
        //这里需要使用原始的mongoDB，否则会有太多冲突
        if (mongoClient instanceof SidistranMongoClient) {
            mongoClient = ((SidistranMongoClient) mongoClient).returnOriClient();
        }
        this.clt = mongoClient.getDB(Constants.TRANSACTION_DB).getCollection(Constants.TRANSACTION_UTIL_CLT);
        this.clt.setWriteConcern(WriteConcern.MAJORITY);
    }

    /**
     * 获取下一个原子时间
     *
     * @return long
     */
    long nextTime() {
        DBObject dbObject = this.clt.findAndModify(new BasicDBObject("_id", ID_GEN),
            null, null, false, new BasicDBObject("$inc", new BasicDBObject(SEQ_FIELD, 1l)),
            true, true);
        return ((Long) dbObject.get(SEQ_FIELD)).longValue();
    }

    /**
     * 获取当前的原子时间
     * @return
     */
    long currentTime(){
        DBObject dbObject = this.clt.findAndModify(new BasicDBObject("_id", ID_GEN),
            null, null, false,
            new BasicDBObject("$inc", new BasicDBObject("_dummy", 1l))
                .append("$setOnInsert", new BasicDBObject(SEQ_FIELD, 1l)),
            true, true);
        return ((Long) dbObject.get(SEQ_FIELD)).longValue();
    }

    /**
     * {@see com.sobey.jcg.sobeyhive.sidistran.mongo2.utils.MongoReadWriteLock.getLockFrom()}
     *
     * @return
     */
    static DBTime getFrom(MongoClient client){
        //一个Client只需要一个
        String key = client.getServerAddressList()+"_"+client.getCredentialsList();
        DBTime instance = map.get(key);
        if(instance!=null){
            return instance;
        }

        instance = new DBTime(client);
        DBTime temp = map.putIfAbsent(key, instance);
        if(temp!=null){
            return temp;
        }else{
            return instance;
        }
    }
}