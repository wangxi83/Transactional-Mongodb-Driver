package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Created by WX on 2016/1/26.
 *
 * 本地的事务ID管理器
 */
class MonTxIDManager{
    private static ConcurrentMap<String, MonTxIDManager> map = new ConcurrentHashMap();
    private DBCollection clt;

    private static String ID_GEN = "ID_Gen";
    private static String SEQ_FIELD = "seq";
    private static String max_tx_field = "max_tx";

    @SuppressWarnings("deprecation")
    private MonTxIDManager(MongoClient mongoClient){
        //这里需要使用原始的mongoDB，否则会有太多冲突
        if(mongoClient instanceof SidistranMongoClient){
            mongoClient = ((SidistranMongoClient)mongoClient).returnOriClient();
        }
        this.clt = mongoClient.getDB(Constants.TRANSACTION_DB).getCollection(Constants.TRANSACTION_UTIL_CLT);
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

//    /**
//     * 使用传入的txid刷新本地最大事务ID
//     * 返回刷新后的txid。
//     * 如果txid比本地的大，则返回的是txid，否则，返回本地的ID
//     *
//     * @param txid 传入的txid
//     * @return
//     */
//    long doMaxIfNot(long txid){
//        DBObject result = this.clt.findAndModify(new BasicDBObject("_id", ID_GEN),
//            null, null, false, new BasicDBObject("$max", new BasicDBObject(SEQ_FIELD, txid)),
//            true,true);
//        return (Long)result.get(SEQ_FIELD);//不可能为空哈
//    }

    /**
     * 获取当前事务所能看到的最大ID
     * @param txid
     * @return long
     */
    long toggleMaxTxID(long txid){
        String ID_TOGGLE = "ID_TOGGLE";
        DBObject dbObject = this.clt.findAndModify(
            new BasicDBObject("_id", ID_TOGGLE),
            null, null, false,
            new BasicDBObject("$max", new BasicDBObject(max_tx_field, txid)),
            true,true);
        return ((Long)dbObject.get(max_tx_field));
    }

    /**
     * 标记分布式事务情况下的服务端ID
     * 只要开启了分布式的，就会记录
     *
     * @param txid
     * @return
     */
    long toggleLastGlobalTxID(long txid){
        String GLOBAL_ID_TOGGLE = "GLOBAL_ID_TOGGLE";
        DBObject dbObject = this.clt.findAndModify(
            new BasicDBObject("_id", GLOBAL_ID_TOGGLE),
            null, null, false,
            new BasicDBObject("$max", new BasicDBObject(max_tx_field, txid)),
            true,true);
        return ((Long)dbObject.get(max_tx_field));
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
