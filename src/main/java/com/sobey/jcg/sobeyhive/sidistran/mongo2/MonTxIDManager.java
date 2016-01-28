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
 *
 * 本地的事务ID管理器
 */
class MonTxIDManager{
    private static ConcurrentMap<String, MonTxIDManager> map = new ConcurrentHashMap();
    private DBCollection clt;

    private static String ID_GEN = "ID_Gen";
    private static String ID_TOGGLE = "ID_TOGGLE";
    private static String SEQ_FIELD = "seq";
    private static String dummyField = "_dummy";
    private static String min_tx_field = "min_tx";
    private static String max_tx_field = "max_tx";

    @SuppressWarnings("deprecation")
    private MonTxIDManager(MongoClient mongoClient){
        //这里需要使用原始的mongoDB，否则会有太多冲突
        if(mongoClient instanceof SidistranMongoClient){
            mongoClient = ((SidistranMongoClient)mongoClient).returnOriClient();
        }
        this.clt = mongoClient.getDB(Constants.TRANSACTION_DB).getCollection(Constants.TRANSACTION_UTIL_CLT);
        this.clt.setWriteConcern(WriteConcern.MAJORITY);
    }

    /**
     * 获取下一个事务ID
     * @return long
     */
    long nextTxID(){
        DBObject dbObject = this.clt.findAndModify(new BasicDBObject("_id", ID_GEN),
            null, null, false, new BasicDBObject("$inc", new BasicDBObject(SEQ_FIELD,  1l)),
            true,true);
        return ((Long)dbObject.get("seq")).longValue();
    }

    void doMaxIfNot(long txid){
        this.clt.findAndModify(new BasicDBObject("_id", ID_GEN),
            null, null, false, new BasicDBObject("$max", new BasicDBObject(SEQ_FIELD, txid)),
            true,true);
    }

    /**
     * 获取当前事务所能看到的最大ID
     * @param txid
     * @return long
     */
    long toggleMaxTxID(long txid){
        DBObject dbObject = this.clt.findAndModify(
            new BasicDBObject("_id", ID_TOGGLE),
            null, null, false,
            new BasicDBObject("$max", new BasicDBObject(max_tx_field, txid)),
            true,true);
        return ((Long)dbObject.get(max_tx_field)).longValue();
    }


    /**
     * {@see com.sobey.jcg.sobeyhive.sidistran.mongo2.utils.MongoReadWriteLock.getLockFrom()}
     *
     * @return
     */
    static MonTxIDManager getFrom(MongoClient client){
        //一个Client只需要一个
        String key = client.getServerAddressList()+"_"+client.getCredentialsList();
        MonTxIDManager instance = map.get(key);
        if(instance!=null){
            return instance;
        }

        instance = new MonTxIDManager(client);
        MonTxIDManager temp = map.putIfAbsent(key, instance);
        if(temp!=null){
            return temp;
        }else{
            return instance;
        }
    }
}
