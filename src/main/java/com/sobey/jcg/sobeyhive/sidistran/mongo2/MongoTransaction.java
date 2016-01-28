package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.Constants.Fields;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.Constants.Values;

/**
 * Created by WX on 2016/1/20.
 *
 * mongodb事务对象
 */
class MongoTransaction{
    protected String stat_f = Fields.ADDITIONAL_BODY+"."+Fields.STAT_FILED_NAME;
    protected String txid_f = Fields.ADDITIONAL_BODY+"."+Fields.TXID_FILED_NAME;
    protected String ufrom_f = Fields.ADDITIONAL_BODY+"."+Fields.UPDATE_FROM_NAME;
    protected String uby_f = Fields.ADDITIONAL_BODY+"."+Fields.UPDATEBY_TXID_NAME;
    protected String time_f = Fields.ADDITIONAL_BODY+"."+Fields.GARBAGE_TIME_NAME;

    protected MongoClient mongoClient;
    protected long txid;
    protected long tx_time;

    protected boolean committed = false;
    protected boolean rollbacked = false;

    protected MongoTransaction(long txid, MongoClient mongoClient){
        this.mongoClient = mongoClient;
        this.txid = txid;
    }

    protected long getTxid() {
        return txid;
    }

    protected void setTx_time(long tx_time) {
        this.tx_time = tx_time;
    }

    protected long getTx_time(){
        return this.tx_time;
    }

    private ConcurrentMap<String, DBCollection> collections = new ConcurrentHashMap<String, DBCollection>();
    protected void addTransactionTargetIfAbsent(DBCollection collection){
        collections.putIfAbsent(collection.getFullName(), collection);
    }

    protected Map<String, DBCollection> getTransactionTargets(){
        return collections;
    }

    protected void onError(Throwable e){
        e.printStackTrace();
        this.rollback();
    }

    @SuppressWarnings("deprecation")
    protected void commit() {
        if(rollbacked){
            throw new IllegalStateException("已经回滚，不能提交");
        }
        if(!collections.isEmpty()){
            //@see MongoReadWriteLock
            MongoReadWriteLock lock = MongoReadWriteLock.getLock(mongoClient);
            try {
                lock.writeLock().lock();

                //把当前事务范围中，增加的部分，修改为可用
                //并且，设置__s_.__s_c_txid为当前可见的最大事务id
                //这样一来，新数据就不会被还没有提交的事务看见 @see SidistranDBCollection.sidistranQueryAdapt()
                long maxtxid = MonTxIDManager.getFrom(mongoClient).toggleMaxTxID(txid);
                DBObject insert2OKQuery = new CommitDBQuery(stat_f, Values.INSERT_NEW_STAT)
                    .append(txid_f, txid);
                DBObject insert2OK =
                    new BasicDBObject("$set", new BasicDBObject(stat_f, Values.COMMITED_STAT)
                        .append(txid_f, maxtxid))
                        .append("$unset", new BasicDBObject(ufrom_f, ""));

                //把那些被当前事务修改的数据
                //更新其“变成垃圾的的日期”，这样，最新的事务就不能看到这些数据，
                //而还没有提交的事务，由于事务时间在“垃圾时间之前”，因此可以看到这些数据
                DBObject expireQuery = new CommitDBQuery(stat_f, Values.COMMITED_STAT)
                    .append(uby_f, txid);
                long time = DBTime.getFrom(mongoClient).nextTime();
                DBObject expire2Garbage = new BasicDBObject("$set", new BasicDBObject(time_f, time));

                for (Iterator<DBCollection> itr = collections.values().iterator();
                     itr.hasNext(); ) {
                    DBCollection clt = itr.next();
                    clt.update(expireQuery, expire2Garbage, false, true, WriteConcern.ACKNOWLEDGED);
                    clt.update(insert2OKQuery, insert2OK, false, true, WriteConcern.ACKNOWLEDGED);
                }
            }finally {
                lock.writeLock().unlock();
                collections.clear();
                DBCollection txClt = this.mongoClient.getDB(Constants.TRANSACTION_DB)
                    .getCollection(Constants.TRANSACTION_TX_CLT);
                txClt.remove(new BasicDBObject("_id", txid));
            }
        }
        committed = true;
    }


    @SuppressWarnings("deprecation")
    protected void rollback(){
        if(committed){
            throw new IllegalStateException("已经提交，不能回滚");
        }
        if(!collections.isEmpty()){
            //把当前事务可见的那些临时数据，设置为需要删除
            DBObject rollbackQuery = new CommitDBQuery(stat_f, Values.INSERT_NEW_STAT)
                .append(txid_f, txid);
            DBObject expire2Garbage = new BasicDBObject("$set", new BasicDBObject(stat_f, Values.NEED_TO_REMOVE));

            for (Iterator<DBCollection> itr = collections.values().iterator();
                 itr.hasNext(); ) {
                DBCollection clt = itr.next();
                clt.update(rollbackQuery, expire2Garbage, false, true, WriteConcern.ACKNOWLEDGED);
            }
            collections.clear();
            DBCollection txClt = this.mongoClient.getDB(Constants.TRANSACTION_DB)
                .getCollection(Constants.TRANSACTION_TX_CLT);
            txClt.remove(new BasicDBObject("_id", txid));
        }
        rollbacked = true;
    }
}
