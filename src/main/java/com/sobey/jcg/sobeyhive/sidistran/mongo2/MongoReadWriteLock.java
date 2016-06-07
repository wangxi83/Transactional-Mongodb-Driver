package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryOperators;
import com.mongodb.WriteConcern;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.ex.LockTimeOutException;

/**
 * Created by WX on 2016/1/22.
 *
 * 基于mongodDB的findAndModify的读写锁
 *
 * 支持replica set
 *
 * 目前粒度比较大，是以每一个DB中的“lock”这个collection来实现。
 * 是一个全局的读写锁
 *
 * 一定要初始化一个名字为lock的collection
 * 并且增加一条数据：
 * {
 *  "_id" : "lock",
 *  "read_count" : NumberLong(0),
 *  "_dummy" : NumberLong(0),
 *  "write_lock" : false
 * }
 *
 *
 * 稍加改造，就可以实现不同Collection的读写锁，也就是支持多把锁
 */
class MongoReadWriteLock {
    private static String locktype_field = "locktype";
    private static String write_lock = "write_lock";
    private static String read_lock = "read_lock";
    private static String ownerField = "owner";
    //单例缓存
    private static ConcurrentMap<String, MongoReadWriteLock> map = new ConcurrentHashMap();

    private DBCollection clt;
    private String key;
    @SuppressWarnings("deprecation")
    private MongoReadWriteLock(MongoClient client){
        //这里需要使用原始的mongoDB，否则会有太多冲突
        if(client instanceof SidistranMongoClient){
            client = ((SidistranMongoClient)client).returnOriClient();
        }

        this.clt = client.getDB(Constants.TRANSACTION_DB).getCollection(Constants.TRANSACTION_UTIL_LOCK);
        this.clt.setWriteConcern(WriteConcern.MAJORITY);
        this.key = client.getServerAddressList()+""+client.getCredentialsList();
        new Thread(){
            public void run(){
                try{
                    List<DBObject> indices = clt.getIndexInfo();
                    boolean hasIndex = false;
                    for(DBObject dbObject : indices) {
                        String name = (String) dbObject.get("name");
                        if (name.equals("locktype_owner")) { //"__s_stat__s_g_time"
                            hasIndex = true;
                            break;
                        }
                    }
                    if(!hasIndex) {
                        clt.createIndex(new BasicDBObject(locktype_field, 1).append(ownerField, 1)
                            , new BasicDBObject("background", true).append("name", "locktype_owner"));
                    }
                }catch (Exception e){
                }
            }
        }.start();
    }

    /**
     * 通过当前DB，获取一个当前DB所在的Client连接的读写锁
     *
     * 意思就是：相同地址的Client，获取的是同一个锁
     *
     * 其实，这个锁应该在初始化的时候获取
     * 其实，这个锁不是“某一个DB”的，而是一个“通用的，借助一个独立的DB，独立的collection产生的”
     * 也就是说，应该是：getLockFrom(client.getDB("util"))
     * 但是，由于util这个库可能不存在，也可能这个client不能访问uti
     * 因此，是：
     *
     * 以 当前client所在的连接中，能正常访问的DB 为单位，获取一个读写锁
     *
     * @return
     */
    static MongoReadWriteLock getLock(MongoClient client){
        //一个Client只需要一个
        String key = client.getServerAddressList()+"_"+client.getCredentialsList();
        MongoReadWriteLock instance = map.get(key);
        if(instance!=null){
            return instance;
        }

        instance = new MongoReadWriteLock(client);
        MongoReadWriteLock temp = map.putIfAbsent(key, instance);
        if(temp!=null){
            return temp;
        }else{
            return instance;
        }
    }

    //--------------------对象访问方法------------------
    protected long timeout = -1l; //默认不超时

    private static ThreadLocal<MonWriteLock> writeLocal = new ThreadLocal<MonWriteLock>();
    private static ThreadLocal<MonReadLock> readLocal = new ThreadLocal<MonReadLock>();

    MonWriteLock writeLock(){
        return writeLock(-1l);
    }

    MonReadLock readLock(){
        return readLock(-1l);
    }

    MonWriteLock writeLock(long timeout){
        MonWriteLock lock = writeLocal.get();
        if(lock!=null){
            return lock;
        }else {
            lock = new MonWriteLock(timeout);
            writeLocal.set(lock);
            return lock;
        }
    }

    MonReadLock readLock(long timeout){
        MonReadLock lock = readLocal.get();
        if(lock!=null){
            return lock;
        }else {
            lock = new MonReadLock(timeout);
            readLocal.set(lock);
            return lock;
        }
    }

    //---------------------基于mongodb的实现------------------
    //获取读数量
    long readCount(){
        String owner =  key+"_"+Thread.currentThread().getName();
        return clt.count(new BasicDBObject(locktype_field, read_lock)
            .append(ownerField, new BasicDBObject(QueryOperators.NE, owner)));
    }

    void addReadLock(){
        String owner =  key+"_"+Thread.currentThread().getName();
        clt.insert(new BasicDBObject(locktype_field, read_lock).append(ownerField, owner));
    }

    void removeReadLock(){
        String owner =  key+"_"+Thread.currentThread().getName();
        clt.remove(new BasicDBObject(locktype_field, read_lock).append(ownerField, owner));
    }

    //获取写锁
    boolean aquireWriteLock(){
        String owner =  key+"_"+Thread.currentThread().getName();
        try{
            clt.insert(new BasicDBObject("_id", write_lock).append(ownerField, owner));
            return true;
        }catch (Exception e){
            if(e instanceof com.mongodb.DuplicateKeyException){
                return false;
            }else{
                if(e instanceof RuntimeException){
                    throw (RuntimeException)e;
                }else{
                    throw new RuntimeException(e);
                }
            }
        }
    }

    //判断是否写锁已加锁
    boolean isWriteLocked(){
        DBObject dbObject = clt.findOne(new BasicDBObject("_id", write_lock));
        if(dbObject==null){
            return false;
        }
        String owner_ = dbObject.get(ownerField)!=null?dbObject.get(ownerField).toString():"";
        String owner =  key+"_"+Thread.currentThread().getName();
        if(owner_.equals(owner)){
            return false;
        }else{
            return true;
        }
    }

    //释放写
    void unlockWrite(){
        String owner =  key+"_"+Thread.currentThread().getName();
        clt.remove(new BasicDBObject("_id", write_lock).append(ownerField, owner));
    }

    //---------------------------------两个实现////////////////////

    //写锁
    class MonWriteLock{
        long timeout = -1l;
        volatile int ref = 0;

        private MonWriteLock(long timeout){
            this.timeout = timeout;
        }

        private void incRef(){
            ref++;
        }

        private void decRef(){
            ref--;
        }

        void lock(){
            int time = 0;
            while(true){
                //是当前线程加的读锁，因此直接抢占写锁
                if(aquireWriteLock()){
                    while(readCount()!=0){
                        if(this.timeout>0) {
                            if (time >= timeout) {
                                throw new LockTimeOutException("写超时，等待读锁超时，timeout="+timeout+", thread:"+Thread.currentThread().getName());
                            }
                            time++;
                            try{Thread.sleep(100);}catch (Exception e){}
                        }
                    }
                    break;
                }


                if(this.timeout>0) {
                    if (time >= timeout) {
                        throw new LockTimeOutException("写超时，等待写锁超时，timeout="+timeout+", thread:"+Thread.currentThread().getName());
                    }
                    time++;
                    try{Thread.sleep(100);}catch (Exception e){}
                }
            }
            incRef();
        }

        void unlock(){
            decRef();
            if(ref==0) {
                writeLocal.remove();
                unlockWrite();
            }
        }
    }

    //读锁
    class MonReadLock{
        long timeout = -1l;
        volatile int ref = 0;
        boolean inc_ed = false;


        private MonReadLock(long timeout){
            this.timeout = timeout;
        }

        private void incRef(){
            ref++;
        }

        private void decRef(){
            ref--;
        }

        void lock(){
            int time = 0;
            while (isWriteLocked()) {
                //只要没有写，就不互斥
                if (this.timeout > 0) {
                    if (time >= timeout) {
                        throw new LockTimeOutException("读超时，等待写锁，timeout=" + timeout+", thread:"+Thread.currentThread().getName());
                    }
                    time++;
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                    }
                }
            }
            incRef();
            if(!inc_ed) {
                addReadLock();
                inc_ed = true;
            }
        }

        void unlock(){
            decRef();
            if(ref==0) {
                readLocal.remove();
                if(inc_ed) {
                    removeReadLock();
                }
            }
        }
    }
}
