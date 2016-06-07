package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryOperators;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.Constants.Fields;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.Constants.Values;

/**
 * Created by WX on 2016/1/28.
 *
 * 定时删除垃圾
 */
class MonTranshCleaner{
    private MongoClient mongoClient;
    private Timer transhAddTimer;
    private Timer transhReleaseTimer;
    private DBCollection trashCollection;

    private static ConcurrentMap<String, MonTranshCleaner> map = new ConcurrentHashMap();

    private MonTranshCleaner(){

    }

    private void init(MongoClient mongoClient){
        if(mongoClient instanceof SidistranMongoClient){
            mongoClient = ((SidistranMongoClient)mongoClient).returnOriClient();
        }
        this.mongoClient = mongoClient;
        this.trashCollection =  mongoClient.getDB(Constants.TRANSACTION_DB)
            .getCollection(Constants.TRANSACTION_TRASH_CLT);
        this.transhAddTimer = new Timer("MonTranshAdd");
        this.transhAddTimer.schedule(transhAdd, 1l, 500l);

        this.transhReleaseTimer = new Timer("MonTranshCleaner");
//        this.transhReleaseTimer.schedule(transhRelease, 1000l, 1800000l);
        this.transhReleaseTimer.schedule(transhRelease, 1l, 60000l);
    }

    /**
     * {@see com.sobey.jcg.sobeyhive.sidistran.mongo2.utils.MongoReadWriteLock.getLockFrom()}
     *
     * @return
     */
    static MonTranshCleaner getFrom(MongoClient client){
        //一个Client只需要一个
        String key = client.getServerAddressList()+"_"+client.getCredentialsList();
        MonTranshCleaner instance = map.get(key);
        if(instance!=null){
            return instance;
        }

        instance = new MonTranshCleaner();
        MonTranshCleaner temp = map.putIfAbsent(key, instance);
        if(temp!=null){
            return temp;
        }else{
            instance.init(client);
            return instance;
        }
    }

    void close(){
        this.transhAddTimer.cancel();
        this.transhReleaseTimer.cancel();
    }

    private ConcurrentHashMap<String, String> transhes = new ConcurrentHashMap();
    void addToTransh(DBCollection collection){
        transhes.put(collection.getName() + "@" + collection.getDB().getName(), "");
    }

    private TimerTask transhAdd = new TimerTask() {
        @Override
        @SuppressWarnings("deprecation")
        public void run() {
            try {
                List<DBObject> list = new ArrayList<>();
                for (Iterator<Entry<String, String>> itr = transhes.entrySet().iterator();
                     itr.hasNext(); ) {
                    Entry<String, String> entry = itr.next();
                    String temp = entry.getKey();
                    String[] cltAndDB = temp.split("@");
                    list.add(new BasicDBObject(cltAndDB[1], cltAndDB[0]));
                    itr.remove();
                }
                if(list!=null&&!list.isEmpty()){
                    trashCollection.insert(list, WriteConcern.ACKNOWLEDGED);
                }
            }catch (Exception e){
                Logger.getLogger(MonTranshCleaner.class.getName()+".transhAdd()")
                    .log(Level.FINEST, ExceptionUtils.getFullStackTrace(e));
            }
        }
    };

    private TimerTask transhRelease = new TimerTask() {
        @Override
        @SuppressWarnings("deprecation")
        public void run() {
            Map<String, HashSet<String>> result = new HashMap<>();
            for (Iterator<DBObject> itr = trashCollection.find(new BasicDBObject(),
                new BasicDBObject("_id", 0)).limit(500).iterator();
                 itr.hasNext(); ) {
                DBObject dbObject = itr.next();
                String DB = dbObject.keySet().iterator().next();
                String colt = (String) dbObject.get(DB);
                HashSet<String> colts = result.get(DB);
                if (colts == null) {
                    colts = new HashSet<>();
                    result.put(DB, colts);
                }
                colts.add(colt);
                trashCollection.remove(dbObject);
            }


            if(!result.isEmpty()){
                try{
                    long minDBTime = findMinDBTimeFromActiveTx();

                    for(Iterator<String> itr = result.keySet().iterator();
                        itr.hasNext();){
                        String dbName = itr.next();
                        DB db = mongoClient.getDB(dbName);
                        HashSet<String> colts = result.get(dbName);
                        for(Iterator<String> itr_ = colts.iterator();
                            itr_.hasNext();){
                            String cltName = itr_.next();
                            DBCollection clt = db.getCollection(cltName);
                            doClean(clt, minDBTime);
                        }
                    }
                }catch (Exception e){
                    Logger.getLogger(MonTranshCleaner.class.getName()+".transhRelease()")
                        .log(Level.FINEST, ExceptionUtils.getFullStackTrace(e));
                }
            }

        }
    };

    /**
     * 删除clt里面的垃圾数据
     *
     * 1,删除那些 状态为-100的数据
     * {__s_.__s_stat: -100}
     *      @see com.sobey.jcg.sobeyhive.sidistran.mongo2.SidistranDBCollection proRollback()
     *      @see com.sobey.jcg.sobeyhive.sidistran.mongo2.SidistranDBCollection remove(DBObject query, WriteConcern writeConcern, DBEncoder encoder)
     *      @see com.sobey.jcg.sobeyhive.sidistran.mongo2.MongoTransaction rollback()
     *
     * 2,
     * def tx = 当前活动事务中，{@see DBTime}最早的事务;
     * 删除那些 比tx的DBTime小的所有过期数据
     * {__s_.__s_g_time < tx.time}
     *      @see com.sobey.jcg.sobeyhive.sidistran.mongo2.SidistranDBCollection sidistranQueryAdapt()
     *      @see MongoTransaction commit()
     * 也就是说，如果数据的过期时间，已经比时间最早的事务还要早，就说明应该删除了
     *
     * @param clt
     */
    private Logger logger = Logger.getLogger(MonTranshCleaner.class.getName());
    private void doClean(DBCollection clt, long dbTime){
        String stat_f = Fields.ADDITIONAL_BODY + "." + Fields.STAT_FILED_NAME;
        String db_time_f = Fields.ADDITIONAL_BODY + "." + Fields.GARBAGE_TIME_NAME;
        DBObject query = new BasicDBObject("$or", new BasicDBObject[]{
            new BasicDBObject(stat_f, Values.NEED_TO_REMOVE),
            new BasicDBObject(db_time_f, new BasicDBObject(QueryOperators.LTE, dbTime))
        });
        WriteResult result = clt.remove(query);
        if(logger.isLoggable(Level.INFO)){
            logger.log(Level.INFO, "[MonTranshCleaner]本次清理："+result.getN()+" 个垃圾数据");
        }
    }

    /**
     * 获取时间最早的活动事务
     * @return
     */
    private long findMinDBTimeFromActiveTx(){
        String time_f = "time";
        DBCursor cursor = mongoClient.getDB(Constants.TRANSACTION_DB)
            .getCollection(Constants.TRANSACTION_TX_CLT)
            .find(new BasicDBObject(time_f, new BasicDBObject("$exists", true)))
            .sort(new BasicDBObject(time_f, 1))
            .limit(1);
        if(cursor!=null&&cursor.hasNext()){
            return ((Long)cursor.next().get(time_f)).longValue();
        }else{
            return DBTime.getFrom(mongoClient).currentTime();
        }
    }
}
