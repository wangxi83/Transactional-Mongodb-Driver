package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.MethodUtils;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import com.mongodb.InsertOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.QueryOperators;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.Constants.Fields;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.Constants.Values;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.ex.SidistranMongoCuccrentException;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.utils.CollectionListConfiger;


/**
 * Created by WX on 2015/12/29.
 *
 * 由于DB被代理了{@see com.sobey.jcg.sobeyhive.sidistran.mongo.SidistranMongoDB}。
 * 并且，其中的executor也被hack了。因此
 * 这里可以大胆的继承。因为executor会被传递到这里
 *
 * TODO:1.有没有更好的办法处理镜像？
 * TODO:2.删除逻辑还有待优化--如果tx1<tx2，那么，tx2的删除是允许覆盖tx1的处理的。目前是冲突报错
 * TODO:3.Array的处理
 */
public class SidistranDBCollection extends DBCollection {
    private Logger logger = LoggerFactory.getLogger(SidistranDBCollection.class);

    private String stat_f = Fields.ADDITIONAL_BODY+"."+Fields.STAT_FILED_NAME;
    private String txid_f = Fields.ADDITIONAL_BODY+"."+Fields.TXID_FILED_NAME;
    private String u_by_f = Fields.ADDITIONAL_BODY+"."+Fields.UPDATEBY_TXID_NAME;
    private String db_time_f = Fields.ADDITIONAL_BODY+"."+Fields.GARBAGE_TIME_NAME;
    private String id_f = Fields.ADDITIONAL_BODY+"."+Fields.ID_FIELD;


    //2016-3-16 wx 这几个索引名字错了。需要重建。用old来指定删除这些垃圾
    private String[] old_indices = new String[]{
        Fields.STAT_FILED_NAME+Fields.GARBAGE_TIME_NAME,
        Fields.STAT_FILED_NAME+Fields.TXID_FILED_NAME +Fields.UPDATEBY_TXID_NAME+Fields.GARBAGE_TIME_NAME,
        Fields.TXID_FILED_NAME+Fields.STAT_FILED_NAME,
        Fields.STAT_FILED_NAME+Fields.TXID_FILED_NAME+Fields.UPDATEBY_TXID_NAME,
        Fields.STAT_FILED_NAME+Fields.UPDATEBY_TXID_NAME,
        Fields.STAT_FILED_NAME,
        Fields.GARBAGE_TIME_NAME
    };

    //新的索引名字
    private String new_1 = "new_"+Fields.STAT_FILED_NAME+Fields.GARBAGE_TIME_NAME;
    private String new_2 = "new_"+Fields.STAT_FILED_NAME+Fields.TXID_FILED_NAME
        +Fields.UPDATEBY_TXID_NAME+Fields.GARBAGE_TIME_NAME;
    private String new_3 = "new_"+Fields.TXID_FILED_NAME+Fields.STAT_FILED_NAME;
    private String new_4 = "new_"+Fields.STAT_FILED_NAME+Fields.TXID_FILED_NAME+Fields.UPDATEBY_TXID_NAME;
    private String new_5 = "new_"+Fields.STAT_FILED_NAME+Fields.UPDATEBY_TXID_NAME;
    private String new_6 = "new_"+Fields.STAT_FILED_NAME;
    private String new_7 = "new_"+Fields.GARBAGE_TIME_NAME;
    private String new_8 = "new_"+Fields.ID_FIELD;


    private static List<String> ops = Arrays.asList(
        "$inc","$mul","$rename","$set","$unset","$min","$max","$currentDate",
        "$addToSet","$pop","$pullAll","$pull","$pushAll","$push"
    );


    private boolean indexEnsured;
    private MongoReadWriteLock readWriteLock;

    private Map<String, List<String>> uniqueIndiceFields = new HashMap<String, List<String>>();

    SidistranDBCollection(DB database, String name) {
        super(database, name);
        if(!indexEnsured&& CollectionListConfiger.needRefreshIndex(database.getName(), name)) {
            for(String old : old_indices){
                try {
                    super.dropIndex(old);
                }catch (Exception e){
                }
            }
            List<DBObject> indices = this.getIndexInfo();
            assureIndex(indices);
            try {
                refreshUniqueFields(indices);
            }catch (MongoCommandException e){
                if(e.getCode()!=12587){
                    // { "errmsg" : "exception: cannot perform operation: a background operation is currently running for collection", "code" : 12587, "ok" : 0.0 }
                    throw e;
                }
            }
            indexEnsured = true;
        }
//        readWriteLock = MongoReadWriteLock.getLock((MongoClient) database.getMongo());
    }

    //region 自动处理index
    private void assureIndex(List<DBObject> indices){
        boolean commonqueryIndex = false; //@see this.commonQueryAdatp()
        boolean sidistranQueryIndex_1 = false; //@see this.sidistranQueryAdapt(), MongoTransaction.commit()
        boolean sidistranQueryIndex_2 = false; //@see this.sidistranQueryAdapt(), MongoTransaction.rollback()
        boolean findCommonOnUpdateQueryIndex = false; //@see this.findCommonDataQuery_OnSidistranUpdate()
        boolean expireQueryIndex = false; //@see MongoTransaction.commit()
        boolean statIndex = false; //@see MonTranshCleaner.doClean()
        boolean gtimeIndex = false; //@see MonTranshCleaner.doClean()
        boolean idIndex = false;

        for(DBObject dbObject : indices){
            String name = (String)dbObject.get("name");
            if(name.equals(new_1)){ //"__s_stat__s_g_time"
                commonqueryIndex = true;
            }
            if(name.equals(new_2)){ //"__s_stat__s_c_txid__s_u_txid__s_g_time"
                sidistranQueryIndex_1 = true;
            }
            if(name.equals(new_3)){ //"__s_c_txid__s_stat"
                sidistranQueryIndex_2 = true;
            }
            if(name.equals(new_4)){ //"__s_stat__s_c_txid__s_u_txid"
                findCommonOnUpdateQueryIndex = true;
            }
            if(name.equals(new_5)){ //"__s_stat__s_u_txid"
                expireQueryIndex = true;
            }
            if(name.equals(new_6)){//"__s_stat"
                expireQueryIndex = true;
            }
            if(name.equals(new_7)){ //"__s_g_time"
                gtimeIndex = true;
            }
            if(name.equals(new_8)){//"__s_id"
                idIndex = true;
            }
        }

        // backgournd的设置，在这里没有起到预想的作用。即使设置了，还是会等待服务器返回而不是异步处理
        // 由于AsyncOperationExecutor测试后，发现不起作用，因此只有自己用线程来做。
        if(!commonqueryIndex){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(new BasicDBObject(stat_f, 1).append(db_time_f, 1)
                            , new BasicDBObject("background", true).append("name", new_1));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
        if(!sidistranQueryIndex_1){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(
                            new BasicDBObject(stat_f, 1)
                                .append(txid_f, 1)
                                .append(u_by_f, 1)
                                .append(db_time_f, 1)
                            ,
                            new BasicDBObject("background", true).append("name", new_2));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
        if(!sidistranQueryIndex_2){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(new BasicDBObject(txid_f, 1).append(stat_f, 1)
                            , new BasicDBObject("background", true).append("name", new_3));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
        if(!findCommonOnUpdateQueryIndex){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(
                            new BasicDBObject(stat_f, 1)
                                .append(txid_f, 1)
                                .append(u_by_f, 1)
                            ,
                            new BasicDBObject("background", true)
                                .append("name", new_4));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
        if(!expireQueryIndex){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(new BasicDBObject(stat_f, 1).append(u_by_f, 1)
                            , new BasicDBObject("background", true).append("name", new_5));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
        if(!statIndex){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(new BasicDBObject(stat_f, 1)
                            , new BasicDBObject("background", true).append("name", new_6));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
        if(!gtimeIndex){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(new BasicDBObject(db_time_f, 1)
                            , new BasicDBObject("background", true).append("name", new_7));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
        if(!idIndex){
            new Thread(){
                public void run(){
                    try {
                        SidistranDBCollection.super.createIndex(new BasicDBObject(id_f, 1)
                            , new BasicDBObject("background", true).append("name", new_8));
                    }catch (Exception e){
                    }
                }
            }.start();
        }
    }

    @Override
    /**
     * 加入唯一鍵輔助字段
     */
    public void createIndex(DBObject keys, DBObject options) {
        Boolean unique;
        try {
            unique = (Boolean) options.get("unique");
        }catch (ClassCastException e){
            Integer i = (Integer) options.get("unique");
            unique = i!=null&&i==1;
        }
        
        String name = (String)options.get("name");
        if(unique!=null&&unique) {
            //如果是创建一个唯一键，则需要判断：
            //是否已经存在keys需求的唯一键了
            //如果满足，则检查是否包含Fields.UNIQUE_，如果不包含，则重新创建
            //如果不满足，则加上Fields.UNIQUE_后创建
            List<DBObject> indices = this.getIndexInfo();
            boolean matchInDB = false;
            boolean needmodify = false;
            DBObject keyInDB = null;
            for(DBObject index : indices) {
                //寻找已经存在的唯一键
                keyInDB = (DBObject) index.get("key");
                String nameInDB = (String)index.get("name");
                //获取唯一键
                Boolean uniqueInDB = (Boolean) index.get("unique");
                if (uniqueInDB != null && uniqueInDB){
                    boolean containsUnqiue = keyInDB.containsField(Fields.UNIQUE_);
                    if(containsUnqiue){
                        keyInDB.removeField(Fields.UNIQUE_);
                    }
                    if(CollectionUtils.isEqualCollection(keys.keySet(), keyInDB.keySet())) {
                        //如果已经存在这个唯一键，并且不需要modify，则退出了
                        matchInDB = true;
                        needmodify = !containsUnqiue;
                        name = nameInDB;
                        break;
                    }
                }
            }
            //要是上面没有退出，则说明需要创建或更新唯一键
            if(!matchInDB){
                //首要的判断是是否已经存在，如果不存在则新建
                createUniquIndexWithUniqueField(name, keys);
            }else if(needmodify){
                //如果存在，就看是否需要刷新。如果又存在又不需要刷新，则任何事情都不做
                super.dropIndex(name);//此处肯定存在，不会报错
                createUniquIndexWithUniqueField(name, keyInDB);
            }
        }else{
            super.createIndex(keys, options);
        }
    }

    private void refreshUniqueFields(List<DBObject> indices){
        for(DBObject index : indices) {
            DBObject key = (DBObject) index.get("key");
            //获取唯一键。在获取快照的时候需要使用
            Boolean unique = (Boolean) index.get("unique");
            String name = (String)index.get("name");
            if (unique != null && unique) {
                if(!key.containsField(Fields.UNIQUE_)){
                    super.dropIndex(name);
                }
                createUniquIndexWithUniqueField(name, key);
            }
        }
    }

    private void createUniquIndexWithUniqueField(String indexName, DBObject indexKeys){
        List<String> uniqueFieldsInIndex = uniqueIndiceFields.get(indexName);
        if(uniqueFieldsInIndex==null){
            uniqueFieldsInIndex = new ArrayList<String>();
            uniqueIndiceFields.put(indexName, uniqueFieldsInIndex);
        }

        BasicDBObject temp = new BasicDBObject();
        for (Iterator<String> itr_ = indexKeys.keySet().iterator();
             itr_.hasNext(); ) {
            String uniqueField = itr_.next();
            temp.put(uniqueField, 1);
            uniqueFieldsInIndex.add(uniqueField);
        }

        if(!indexKeys.containsField(Fields.UNIQUE_)) {
            //如果唯一键里面不包含这个辅助字段，则加上
            temp.put(Fields.UNIQUE_, 1);
        }
        super.createIndex(temp,
            new BasicDBObject("unique", 1).append("name", indexName).append("background", 1));
        uniqueFieldsInIndex.add(Fields.UNIQUE_);
    }
    //endregion

    //region ----------------insert-----------------------
    /**
     * 所有的insert都会走到这个方法，因此覆盖此方法
     */
    @Override
    public WriteResult insert(List<? extends DBObject> documents, InsertOptions insertOptions) {
        if (MongoTanscationManager.hasTransaction()) {
            long txid = MongoTanscationManager.txid_AtThisContext();
            /**
             * 如果是在sidistran环境下，则通过版本来控制可见性 {@see this.find()}
             * 通过增加一个版本字段来控制可见性
             */
            for (DBObject cur : documents) {
                if (cur.get(Fields.ADDITIONAL_BODY) == null) {
                    DBObject body = new BasicDBObject();
                    //stat用于控制可见性
                    body.put(Fields.STAT_FILED_NAME, Values.INSERT_NEW_STAT);
                    //txid用于控制事务范围
                    body.put(Fields.TXID_FILED_NAME, txid);
                    //这里开始转换_id
                    Object _id = cur.get(ID_FIELD_NAME);
                    if(_id==null){
                        //id为空，则生成
                        _id = new ObjectId();
                        cur.put(ID_FIELD_NAME, _id);
                    }
                    body.put(Fields.ID_FIELD, _id);//设置到body中

                    cur.put(Fields.ADDITIONAL_BODY, body);
                }
            }
            MongoTanscationManager.current().addTransactionTargetIfAbsent(this);
        } else {
            for (DBObject cur : documents) {
                if (cur.get(Fields.ADDITIONAL_BODY) == null) {
                    DBObject body = new BasicDBObject();
                    //stat用于控制可见性
                    body.put(Fields.STAT_FILED_NAME, Values.COMMITED_STAT);
                    body.put(Fields.GARBAGE_TIME_NAME, Long.MAX_VALUE);
                    body.put(Fields.TXID_FILED_NAME, -1l);
                    body.put(Fields.UPDATEBY_TXID_NAME, -1l);
                    //这里开始转换_id
                    Object _id = cur.get(ID_FIELD_NAME);
                    if(_id==null){
                        //id为空，则生成
                        _id = new ObjectId();
                        cur.put(ID_FIELD_NAME, _id);
                    }
                    body.put(Fields.ID_FIELD, _id);//设置到body中
                    cur.put(Fields.ADDITIONAL_BODY, body);
                }
            }
        }
        try {
            return super.insert(documents, insertOptions);
        }catch (Exception e){
            if(MongoTanscationManager.hasTransaction()){
                MongoTanscationManager.transactionOnError(e);
            }
            throw e;
        }
    }

    /**
     * 重写save。不能出现upsert的情况
     * @param document
     * @param writeConcern
     * @return
     */
    @Override
    public WriteResult save(DBObject document, WriteConcern writeConcern) {
        Object id = document.get(ID_FIELD_NAME);
        if (id == null) {
            return insert(document, writeConcern);
        } else {
            //sidistran的表，id在返回的时候，都转换成了__s_._id
            DBObject filter = new BasicDBObject(id_f, id);
            return this.update(filter, document, true, false, writeConcern, null);
        }
    }
    //endregion

    //region ----------------remove-----------------------
    /**
     * 所有的remove都会走到这个方法，因此覆盖此方法
     *
     * 删除变成逻辑删除
     */
    @Override
    public WriteResult remove(DBObject query, WriteConcern writeConcern) {
        return this.remove(query, writeConcern, null);
    }
    @Override
    public WriteResult remove(DBObject query) {
        return this.remove(query, WriteConcern.ACKNOWLEDGED);
    }
    @Override
    public WriteResult remove(DBObject query, WriteConcern writeConcern, DBEncoder encoder) {
        if (MongoTanscationManager.hasTransaction()) {
            //在sidistran环境下，删除变成逻辑删除
            //这里一定要使用this的，因为是版本事务控制
            //query会在update中处理
            try {
                WriteResult result = this.update(query, new BasicDBObject(), false, true,
                    writeConcern, encoder, UpdateType.REMOVE);
                return result;
            } catch (Exception e) {
                MongoTanscationManager.transactionOnError(e);
                throw e;
            }
        } else {
            //正常模式下，不能看到sidistran的数据
            commonQueryAdapt(query);
            return super.remove(query, writeConcern, encoder);
        }
    }
    //endregion


    //region ----------------count-----------------------
    /**
     * 同上
     */
    @Override
    public long getCount(DBObject query, DBObject projection, long limit, long skip,
                         ReadPreference readPreference) {
        if (MongoTanscationManager.hasTransaction()) {
            sidistranQueryAdapt(query);
        } else {
            //正常模式下，不能看到sidistran的数据
            commonQueryAdapt(query);
        }
        projection = new BasicDBObject(ID_FIELD_NAME, 1);

        try {
            //readWriteLock.readLock().lock();
            return super.getCount(query, projection, limit, skip, readPreference);
        }finally {
            //readWriteLock.readLock().unlock();
        }
    }
    //endregion


    //region ----------------find-----------------------
    /**
     * 查询方法，都需要覆盖
     */
    @Override
    public DBCursor find(DBObject query) {
        return this.find(query, null);
    }
    @Override
    public DBCursor find(DBObject query, DBObject projection) {
        if (MongoTanscationManager.hasTransaction()) {
            sidistranQueryAdapt(query);
        } else {
            //正常模式下，不能看到sidistran的数据
            commonQueryAdapt(query);
        }
        if (projection == null) {
            projection = new BasicDBObject();
        }
        projectionAdapt(projection);

        //DBDBCursor是一个服务端远程指针，实际上是用Collection的find方法
        //最终通过Excecutor的connection和服务端通讯取得next
        //因此，这里不能用super的，要new一个，把this传递进去
        try {
            //readWriteLock.readLock().lock();
            DBCursor cursor = new SidistranDBCursor(this, query, projection, getReadPreference());
            return cursor;
        }finally {
            //readWriteLock.readLock().unlock();
        }
    }
    @Override
    public DBObject findOne(DBObject query, DBObject projection, DBObject sort,
                            ReadPreference readPreference) {
        if (MongoTanscationManager.hasTransaction()) {
            sidistranQueryAdapt(query);
        } else {
            //正常模式下，不能看到sidistran的数据
            commonQueryAdapt(query);
        }
        if (projection == null) {
            projection = new BasicDBObject();
        }
        projectionAdapt(projection);

        try {
            //readWriteLock.readLock().lock();
            DBObject dbObject = super.findOne(query, projection, sort, readPreference);
            updateResult(dbObject);
            return dbObject;
        }finally {
            //readWriteLock.readLock().unlock();
        }
    }
    @Override
    public List distinct(String fieldName, DBObject query, ReadPreference readPreference) {
        //2016-03-18 暂时注释屌。
        //开启query的条件后的时间，超级慢。
        //而目前的分析是：distinct一般不会传条件，因此暂时注释
//        if(MongoTanscationManager.hasTransaction()) {
//            sidistranQueryAdapt(query);
//        }else{
//            //正常模式下，不能看到sidistran的数据
//            commonQueryAdapt(query);
//        }
        try {
            //readWriteLock.readLock().lock();
            return super.distinct(fieldName, query, readPreference);
        }finally {
            //readWriteLock.readLock().unlock();
        }
    }

    //endregion

    //region ----------------update-----------------------
    /**
     * update的处理都会走到这里
     */
    @Override
    public WriteResult update(DBObject query, DBObject update, boolean upsert, boolean multi,
                              WriteConcern aWriteConcern, DBEncoder encoder) {
        return this.update(query, update, upsert, multi, aWriteConcern, encoder, UpdateType.UPDATE);
    }

    private WriteResult update(DBObject query, DBObject update, boolean upsert, boolean multi,
                              WriteConcern aWriteConcern, DBEncoder encoder, UpdateType updateType) {
        WriteResult result = null;
        try {
            //readWriteLock.readLock().lock();
            if (query instanceof CommitDBQuery) {
                return super.update(query, update, upsert, multi, aWriteConcern, encoder);
            } else if (MongoTanscationManager.hasTransaction()) {
                MongoTanscationManager.current().addTransactionTargetIfAbsent(this);
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        "【sidistran_mongo_dbcollection】【事务环境】" +
                        (updateType == UpdateType.REMOVE ? "remove" : "update")+
                         ", 条件为："+query+", update："+update+", tx="+MongoTanscationManager.current()
                    );
                }

                long txid = MongoTanscationManager.txid_AtThisContext();
                //--------------warning!!!效率影响问题开始------------------
                //这里有一个【非可靠】的乐观锁处理：
                //1、先读取事务可见性中的待处理（所谓待处理就是值得当前事务中看到的stat=2的数据）数据，获取countA
                //2、update这些数据，设置这些数据为当前事务所处理
                //   update的条件为：__s_u_txid不存在
                //3、由于[幂等性]，1和2返回的count应该一致。

                //1.当前事务可见的“待处理”数据，也就是 当前事务.可见的.那些已提交的数据
                DBObject query_4_common = new BasicDBObject(query.toMap());//clone一个出来
                snapshotQuery_OnSidistranUpdate(query_4_common);//这个查询保证[幂等性]

                //1.快照获取。复制一份数据版本的快照出来，这些数据指的是query覆盖的普通数据
                DBCursor cursor = super.find(query_4_common);//调用super的查询，原汁原味
                int findCount = cursor.count();
                if (findCount > 0) {
                    long time = System.currentTimeMillis();
                    if (logger.isDebugEnabled()) {
                        logger.debug("【sidistran_mongo_dbcollection】【事务环境】update，开始获取快照，快照数量：" + findCount
                            +", tx="+MongoTanscationManager.current());
                    }
                    List<DBObject> list = new ArrayList<>();
                    while (cursor.hasNext()) {
                        DBObject dbObject = cursor.next();
                        //将update的数据复制一份出来进行处理
                        if (!dbObject.containsField(Fields.ADDITIONAL_BODY)) {
                            throw new UnsupportedOperationException("无法处理数据，因为没有" + Fields.ADDITIONAL_BODY);
                        }

                        DBObject body = (DBObject) dbObject.get(Fields.ADDITIONAL_BODY);
                        if (body.get(Fields.UPDATEBY_TXID_NAME) != null) {
                            long locker = (long) body.get(Fields.UPDATEBY_TXID_NAME);
                            if (locker != txid && locker != -1l) {
                                proRollback(encoder);
                                throw new SidistranMongoCuccrentException("could not serialize access due to concurrent update." +
                                    " cur.txid=" + txid + " , _id=" + dbObject.get("_id") + ", locker=" + body.get(Fields.UPDATEBY_TXID_NAME));
                            }
                        }

                        body = new BasicDBObject();
                        int stat = updateType == UpdateType.UPDATE ? Values.INSERT_NEW_STAT : Values.NEED_TO_REMOVE;
                        if(updateType == UpdateType.UPDATE) {
                            dbObject.put(Fields.ADDITIONAL_BODY, body);
                            body.put(Fields.TXID_FILED_NAME, txid);
                            body.put(Fields.STAT_FILED_NAME, stat);
                            //这个就是原始ID.
                            body.put(Fields.ID_FIELD, dbObject.get("_id"));
                            dbObject.removeField("_id");
                            //这个字段用来避免唯一键冲突
                            //@see this.refreshUniqueFields();
                            dbObject.put(Fields.UNIQUE_, Values.UNIQUE_VAL_SNAPSHOT);
                            list.add(dbObject);
                        }
                    }


                    //然后，对原始数据增加当前事务标识。
                    //这里需要处理的，只能是被update的原始数据
                    DBObject update_4_common = this.getOriDataUpdateObj();//set事务标识
                    query_4_common = new BasicDBObject(query.toMap());//资源竞争查询
                    findCommonDataQuery_OnSidistranUpdate(query_4_common);

                    //开始竞争update资源
                    boolean win = false;
                    long lastCount = 0;
                    //每个update事务，都要去竞争目标资源
                    //修改目标资源的u_txid为当前事务。
                    while(!win) {
                        WriteResult wr = super.update(query_4_common, update_4_common, false, true, WriteConcern.ACKNOWLEDGED, encoder);
                        long count = lastCount+wr.getN();
                        //判断是否赢了
                        win = count == findCount;
                        if(win){
                            break;
                        }else{
                            if(wr.getN()==0){ //第一轮就没竞争到就输
                                proRollback(encoder); //放弃抢占的资源
                                throw new SidistranMongoCuccrentException("could not serialize access due to concurrent update." +
                                    " cur.txid=" + txid);
                            }else if(
                                ((findCount % 2 == 0)&&(count<findCount/2)) //如果是偶数个资源，那么小于一半也输
                                    ||
                                    ((findCount % 2>0)&&(count<findCount/2+1))//如果是奇数个资源，那么小于一半也输
                                ){
                                proRollback(encoder); //放弃抢占的资源
                                throw new SidistranMongoCuccrentException("could not serialize access due to concurrent update." +
                                    " cur.txid=" + txid);
                            }
                            //只有赢家会胜出
                            lastCount += wr.getN();
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("【sidistran_mongo_dbcollection】【在事务环境】update即将写入快照:"+ list.size()+ ", tx="+MongoTanscationManager.current());
                    }

                    //复制快照数据
                    //TODO：也许有一个更好的办法来处理快照。可以试试setOnInsert
                    //@see https://docs.mongodb.org/manual/reference/operator/update/setOnInsert/#up._S_setOnInsert
                    if(list.size()>0) {
                        super.insert(list, new InsertOptions().writeConcern(aWriteConcern).dbEncoder(encoder));
                    }

                    if (logger.isDebugEnabled()) {
                        logger.debug("【sidistran_mongo_dbcollection】在事务环境下update，快照处理完成，time=" + (System.currentTimeMillis() - time));
                    }
                }

                //对复制出来的数据，当前事务中的insert数据、update数据，进行真正的更新
                sidistranQueryAdapt(query);
                //这里需要的是对remove的处理。如果是当前事务中的insert数据、update数据，直接用原始请求更新即可
                if (updateType == UpdateType.REMOVE) {
                    update = this.getRemoveUpdateObj();
                    if (logger.isDebugEnabled()) {
                        logger.debug("【sidistran_mongo_dbcollection】事务环境下remove, 转换的update="+update);
                    }
                } else {
                    //对update进行处理，如果里面包含唯一键的字段，则需要把Fields.UNIQUE_去掉，如果出现报错，说明唯一键冲突
                    uniqueIndexFieldProcess(update, true);
                    if (logger.isDebugEnabled()) {
                        logger.debug("【sidistran_mongo_dbcollection】事务环境下update, 转换的update="+update);
                    }
                }

                result = super.update(query, update, upsert, multi, aWriteConcern, encoder);
                return result;
            } else {
//                //如果是upsert，则需要单独处理
//                if (upsert) {
//                    //原汁原味调用
//                    long count = super.find(query, new BasicDBObject("_id", 1)).limit(1).count();
//                    if(count==0){
//                        DBObject dbObject = this.findAndModify(query, null, null, false, update, true, true, aWriteConcern);
//                        WriteResult result = new WriteResult(1, true, dbObject.get("_id"));
//                        return result;
//                    }
//                }
                //普通情况下，直接执行
                commonQueryAdapt(query);
                uniqueIndexFieldProcess(update, false);
                if (logger.isDebugEnabled()) {
                    logger.debug("【sidistran_mongo_dbcollection】非事务环境update, 转换的update="+update);
                }
                result = super.update(query, update, upsert, multi, aWriteConcern, encoder);
                return result;
            }
        } catch (Exception e) {
            if (MongoTanscationManager.hasTransaction()) {
                MongoTanscationManager.transactionOnError(e);
            }
            throw e;
        } finally {
            //readWriteLock.readLock().unlock();
            if(result!=null&&result.getUpsertedId()!=null){
                try {
                    super.findAndModify(
                            new BasicDBObject(ID_FIELD_NAME, result.getUpsertedId()),
                            null, null, false,
                            new BasicDBObject("$set", new BasicDBObject(id_f, result.getUpsertedId())),
                            false, false, WriteConcern.ACKNOWLEDGED);
                }catch (MongoCommandException e){
                    if(e.getCode()==61){
                        String shardKey = CollectionListConfiger.getShardKey(this.getDB().getName(), this.getName());
                        if (StringUtils.isEmpty(shardKey)) {
                            throw new UnsupportedOperationException("Sidistran环境下进行__s_id补录，当前Collection["
                                    +this.getName()+"]的ShardKey的没有在sidistran_collections.properties中配置");
                        }
                        String[] keys = shardKey.split("\\.");
                        Object value = update.get(keys[0]);
                        if(value!=null){
                            for(int i=1; i<keys.length; i++){
                                if(value!=null&&value instanceof DBObject) {
                                    value = ((DBObject)value).get(keys[i]);
                                }
                            }
                        }
                        if(value==null){
                            throw new UnsupportedOperationException("当前Collection["+this.getName()
                                    +"]的ShardKey["+shardKey+"]的值为空");
                        }
//                        super.findAndModify(
//                            new BasicDBObject(shardKey, value),
//                            null, null, false,
//                            new BasicDBObject("$set", new BasicDBObject(id_f, oid)),
//                            false, false, WriteConcern.ACKNOWLEDGED);
                        super.findAndModify(
                                new BasicDBObject(shardKey, value),
                                null, null, false,
                                new BasicDBObject("$set", new BasicDBObject(id_f, result.getUpsertedId())),
                                false, false, WriteConcern.ACKNOWLEDGED);
                    }
                }
            }
        }
    }

    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort,
                                  boolean  remove,DBObject update, boolean  returnNew,
                                  boolean  upsert,
                                  WriteConcern writeConcern){
        if(fields==null){
            fields = new BasicDBObject();
        }
        findAndModifyAdapt(query, fields, sort, remove, update, upsert);
        DBObject dbObject = super.findAndModify(query, fields, sort, remove, update, returnNew, upsert, writeConcern);
        updateResult(dbObject);
        return dbObject;
    }

    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort,
                                  boolean  remove,DBObject update, boolean  returnNew,
                                  boolean  upsert,
                                  long maxTime, TimeUnit maxTimeUnit) {
        if(fields==null){
            fields = new BasicDBObject();
        }
        findAndModifyAdapt(query, fields, sort, remove, update, upsert);
        DBObject dbObject =  super.findAndModify(query, fields, sort, remove, update, returnNew, upsert, maxTime, maxTimeUnit);
        updateResult(dbObject);
        return dbObject;
    }

    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort,
                                  boolean  remove,DBObject update, boolean  returnNew,
                                  boolean  upsert,
                                  long maxTime, TimeUnit maxTimeUnit, WriteConcern writeConcern) {
        if(fields==null){
            fields = new BasicDBObject();
        }
        findAndModifyAdapt(query, fields, sort, remove, update, upsert);
        DBObject o = super.findAndModify(query, fields, sort, remove, update, returnNew, upsert, maxTime, maxTimeUnit, writeConcern);
        updateResult(o);
        return o;
    }

    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort,
                                  boolean  remove,DBObject update, boolean  returnNew,
                                  boolean  upsert,
                                  boolean bypassDocumentValidation, long maxTime, TimeUnit maxTimeUnit) {
        if(fields==null){
            fields = new BasicDBObject();
        }
        findAndModifyAdapt(query, fields, sort, remove, update, upsert);
        DBObject o = super.findAndModify(query, fields, sort, remove, update, returnNew, upsert,
            bypassDocumentValidation, maxTime, maxTimeUnit);
        updateResult(o);
        return o;
    }

    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort,
                                  boolean  remove,DBObject update, boolean  returnNew,
                                  boolean  upsert,
                                  boolean bypassDocumentValidation, long maxTime,
                                  TimeUnit maxTimeUnit,   WriteConcern writeConcern) {
        if(fields==null){
            fields = new BasicDBObject();
        }
        findAndModifyAdapt(query, fields, sort, remove, update, upsert);
        DBObject o = super.findAndModify(query, fields, sort, remove, update, returnNew, upsert,
            bypassDocumentValidation, maxTime, maxTimeUnit, writeConcern);
        updateResult(o);
        return o;
    }

    private void findAndModifyAdapt(DBObject query, DBObject fields, DBObject sort,
                                    boolean  remove,DBObject update, boolean upsert){
        projectionAdapt(fields);
        if (MongoTanscationManager.hasTransaction()) {
            MongoTanscationManager.current().addTransactionTargetIfAbsent(this);
            //事务可见性处理
            sidistranQueryAdapt(query);

            long txid = MongoTanscationManager.txid_AtThisContext();
            //对原始数据增加事务标识
            DBObject update_4_common = this.getOriDataUpdateObj();//增加事务标识
            DBObject query_4_common = new BasicDBObject(query.toMap());//clone一个出来
            findCommonDataQuery_OnSidistranUpdate(query_4_common);

            DBObject dbObject = super.findAndModify(query, fields, sort, false, update_4_common, true, false, this.getWriteConcern());
            if (dbObject == null) {
                //如果returnNew==null，说明已经被其他事务更新了
                throw new SidistranMongoCuccrentException("could not serialize access due to concurrent update." +
                    " cur.txid=" + txid + " , _id=" + dbObject.get("_id"));
            }

            if (!remove) {
                //如果不删除，则生成快照
                BasicDBObject body = new BasicDBObject();
                dbObject.put(Fields.ADDITIONAL_BODY, body);
                body.put(Fields.TXID_FILED_NAME, txid);
                body.put(Fields.STAT_FILED_NAME, Values.INSERT_NEW_STAT);
                body.put(Fields.UPDATE_FROM_NAME, dbObject.get("_id"));
                //这个字段用来避免唯一键冲突
                //@see this.refreshUniqueFields();
                dbObject.put(Fields.UNIQUE_, Values.UNIQUE_VAL_SNAPSHOT);
                dbObject.removeField("_id");
                super.insert(dbObject, this.getWriteConcern());
            }

            //对update进行处理，如果里面包含唯一键的字段，则需要把Fields.UNIQUE_去掉，如果出现报错，说明唯一键冲突
            uniqueIndexFieldProcess(update, true);
        } else {
            commonQueryAdapt(query);
            uniqueIndexFieldProcess(update, false);
        }
    }
    //endregion

    //region ----------------aggregate-----------------------
    @Override
    public Cursor aggregate(List<? extends DBObject> pipeline, AggregationOptions options,
                            ReadPreference readPreference) {
        if (MongoTanscationManager.hasTransaction()) {
            pipeLineAdapt(pipeline, true);
        } else {
            pipeLineAdapt(pipeline, false);
        }

        try {
            //readWriteLock.readLock().lock();
            Cursor cursor = super.aggregate(pipeline, options, readPreference);
            return new SidistranDBCursor(this, cursor);
        }finally {
            //readWriteLock.readLock().unlock();
        }
    }

    @Override
    public AggregationOutput aggregate(List<? extends DBObject> pipeline, ReadPreference readPreference) {
        try {
            //readWriteLock.readLock().lock();
            AggregationOutput output = super.aggregate(pipeline, readPreference);
            Iterator<DBObject> itr = output.results().iterator();
            while(itr.hasNext()){
                updateResult(itr.next());
            }
            return output;
        }finally {
            //readWriteLock.readLock().unlock();
        }
    }
    //endregion

    //region =--===================查询适配方法，史无前例的华丽分割线--------===========

    /**
     * @see com.sobey.jcg.sobeyhive.sidistran.mongo2.SidistranDBCursor
     * @param object
     */
    void updateResult(DBObject object){
        if(object==null) return;

        boolean has_ID = false;
        Object oid = object.get(ID_FIELD_NAME);
        try {
            if (object.get(Fields.ADDITIONAL_BODY) != null) {
                DBObject body = (DBObject) object.get(Fields.ADDITIONAL_BODY);
                Object _id = body.get(Fields.ID_FIELD);
                if (_id != null) {
                    has_ID = true;
                    object.put("_id", _id); //替换_id为__s_id
                }
                object.removeField(Fields.ADDITIONAL_BODY);
            }
            return;
        }finally {
            if(!has_ID){
                //如果没有__s_id，则说明可能是历史的collection替换成了sidisitran-collection,。
                //这些历史数据是没有__s_id的。
                //这里在查询出来之后加载上这个字段
                //因为一般来说，是不可能无缘无故采用“_id"来做查询的。用"_id"来查询的情况，肯定是已经通过某些条件查询出来之后，才会用
                //因此，能走到这里的，都是采用“非_id的正常查询”查询之后，得到的结果
                try {
                    super.findAndModify(
                        new BasicDBObject(ID_FIELD_NAME, oid),
                        null, null, false,
                        new BasicDBObject("$set", new BasicDBObject(id_f, oid)),
                        false, false, WriteConcern.ACKNOWLEDGED);
                }catch (MongoCommandException e){
                    try{
                        if(e.getCode()==61){
                            String shardKey = CollectionListConfiger.getShardKey(this.getDB().getName(), this.getName());
                            if (StringUtils.isEmpty(shardKey)) {
                                throw new UnsupportedOperationException("Sidistran环境下进行__s_id补录，当前Collection["
                                        +this.getName()+"]的ShardKey的没有在sidistran_collections.properties中配置");
                            }
                            String[] keys = shardKey.split("\\.");
                            Object value = object.get(keys[0]);
                            if(value!=null){
                                for(int i=1; i<keys.length; i++){
                                    if(value!=null&&value instanceof DBObject) {
                                        value = ((DBObject)value).get(keys[i]);
                                    }
                                }
                            }
                            if(value==null){
                                throw new UnsupportedOperationException("当前Collection["+this.getName()
                                        +"]的ShardKey["+shardKey+"]的值为空");
                            }
                            super.findAndModify(
                                    new BasicDBObject(shardKey, value),
                                    null, null, false,
                                    new BasicDBObject("$set", new BasicDBObject(id_f, oid)),
                                    false, false, WriteConcern.ACKNOWLEDGED);
                        }
                    } catch (Exception ee){
                        if(logger.isWarnEnabled()) logger.warn("补录历史数据的__s_id失败:" + ee.getMessage());
                    }
                }
            }
        }
    }


    /**
     * 在非sidistran的情况下，普通查询的内容：
     * 就是那些已经提交过的数据
     * @see com.sobey.jcg.sobeyhive.sidistran.mongo2.MongoTransaction commit()
     * 提交的时候，会把“成为垃圾的时间”设置成Long.MAX_VALUE，即表示永远不是垃圾
     *
     * $and: [
     *       {
     *          __s_.__s_stat: 2, __s_.__s_g_time:Long.MAX_VALUE
     *       }
     * ]
     * @param query
     */
    private void commonQueryAdapt(DBObject query){
        if(logger.isDebugEnabled()){
            logger.debug("【sidistran_mongo_dbcollection】【非事务】环境原始请求：" + query);
        }
        DBObject con = new BasicDBObject(stat_f, Values.COMMITED_STAT)
    .append(db_time_f, Long.MAX_VALUE);
        queryAdapt(query, con);
        if(logger.isDebugEnabled()){
            logger.debug("【sidistran_mongo_dbcollection】【非事务】环境处理后请求："+query);
        }
    }

    /**
     * 在sidistran环境下的query【实现可重复读】
     * 能够看到的数据是：
     * 事务启动那个时刻的已提交数据、当前事务中insert和update的数据
     * 但是不能看到被update复制的原始数据
     *
     * 1、事务启动时刻的已提交数据
     *  {__s_.__s_stat: 2, __s_.__s_c_txid:{$lte: cur.txid}}
     * 2、对于普通数据和当前事务中的insert、update有：
     *  D:-1, C:0, U:1, OK:2
     *  因此，X=2, 就只有普通数据
     *  同时，当前事务中，并且不为-1和2的，就是当前事务中的insert和update的数据
     *  当前事务中不会存在OK的数据
     *  { __s_.__s_c_txid: cur.txid, __s_.__s_stat:{$gt:-1, $lt:2}}//当前事务中的insert和update数据
     *
     *
     * $and: [
     *        {
     *          $or:[
     *           //前序事务创建的，且未修改的
     *            {__s_.__s_stat: 2, __s_.__s_c_txid:{$lt: cur.txid},__s_._s_c_time:{$exists:false}},
     *            //当前事务或上一个事务创建后，被后续事务修改
     *            {__s_.__s_stat: 2, __s_.__s_c_txid:{$lt: cur.txid},__s_.s_u_txid:{$gt:cur.txid},__s_._s_c_time:{$gt:cur.time}},
     *            //由前面的事务创建，被前面的事务修改的
     *            {__s_.__s_stat: 2, __s_.__s_c_txid:{$lt: cur.txid},__s_.s_u_txid:{$lt: cur.txid},__s_._s_c_time:{$gt:cur.time}},
     *            //当前事务中的insert和update数据
     *            {__s_.__s_c_txid: cur.txid, __s_.__s_stat:0}
     *          ]
     *        }
     * ]
     *
     * ----->
     * 上述条件可以简化为：
     *  $and: [
     *        {
     *          $or:[
     *            //被其他事务创建的，且未提交修改的
     *            {__s_.__s_stat: 2, __s_.__s_c_txid:{$lt: cur.txid},__s_.__s_u_txid:{$ne:cur.txid},__s_.__s_c_time:{$gt:cur.time}},
     *            //当前事务中的insert和update数据
     *            {__s_.__s_c_txid: cur.txid, __s_.__s_stat:0}
     *          ]
     *        }
     * ]
     *
     * __s_u_txid的“不等于”的条件，决定了被txid处理的数据，每一次查询不会被查出来
     * 保证了幂等性
     *
     * @param query
     */
    private void sidistranQueryAdapt(DBObject query){
        if(logger.isDebugEnabled()){
            logger.debug("【sidistran_mongo_dbcollection】【事务环境】下原始请求：" + query);
        }
        long txid = MongoTanscationManager.txid_AtThisContext();
        long time = MongoTanscationManager.current().getTx_time();

        DBObject con = new BasicDBObject()
            .append("$or", new BasicDBObject[]{
                new BasicDBObject(stat_f, Values.COMMITED_STAT)
                          .append(txid_f, new BasicDBObject(QueryOperators.LT, txid))
                          .append(u_by_f, new BasicDBObject(QueryOperators.NE, txid))
                          .append(db_time_f, new BasicDBObject(QueryOperators.GT, time))
                ,
                new BasicDBObject(txid_f, txid).append(stat_f, Values.INSERT_NEW_STAT)
            });

        queryAdapt(query, con);

        if(logger.isDebugEnabled()){
            logger.debug("【sidistran_mongo_dbcollection】【事务环境】下处理之后的请求："+query);
        }
    }

    /**
     * update的时候获取快照的查询
     * 这个查询能看到得到是：
     *
     * 在sidistranQueryAdapt的前提下
     * __s_._s_stat=2的数据
     *
     * __s_u_txid的两个“不等于”的条件，决定了被txid处理的数据，每一次查询不会被查出来
     * 保证了幂等性
     *
     * 其实就是：
     *
     *  $and: [
     *        {
     *          __s_.__s_stat: 2,
     *          $or:[
     *            //前序事务创建的，且未修改的
     *            {__s_.__s_c_txid:{$lt: cur.txid},__s_._s_c_time:{$exists:false}},
     *            //当前事务或上一个事务创建后，被后续事务修改，但还没提交的
     *            {__s_.__s_c_txid:{$lt: cur.txid},__s_.s_u_txid:{$gt:cur.txid},__s_._s_c_time:{$gt:cur.time}},
     *            //由前面的事务创建，并且前面的事务还在修改，但还没提交的
     *            {__s_.__s_c_txid:{$lt: cur.txid},__s_.s_u_txid:{$lt: cur.txid},__s_._s_c_time:{$gt:cur.time}},
     *          ]
     *        }
     * ]
     *
     * -----》
     * 可以简化为
     *  $and: [
     *        {
     *          __s_.__s_stat: 2,__s_.__s_c_txid:{$lt: cur.txid},__s_.__s_u_txid:{$ne:cur.txid},__s_.__s_c_time:{$gt:cur.time}
     *        }
     * ]
     *
     *
     * @param query
     */
    private void snapshotQuery_OnSidistranUpdate(DBObject query){
        long txid = MongoTanscationManager.txid_AtThisContext();
        long time = MongoTanscationManager.current().getTx_time();

        DBObject con = new BasicDBObject(stat_f, Values.COMMITED_STAT)
            .append(txid_f, new BasicDBObject(QueryOperators.LT, txid))
            .append(u_by_f, new BasicDBObject(QueryOperators.NE, txid))
            .append(db_time_f, new BasicDBObject(QueryOperators.GT, time));
        queryAdapt(query, con);
    }


    /**
     * update的时候进行乐观锁判定的查询
     * 这个查询能看到得到是：
     * 在sidistranQueryAdapt的前提下
     * __s_._s_stat=2&&__s_.s_u_txid==null的数据
     *
     * 其实就是:
     * 当前事务范围内，还没有__u_txid的数据
     *
     * $and: [
     *        {
     *          __s_.__s_stat: 2, __s_.__s_c_txid:{$lt: cur.txid},__s_.__s_u_txid:{$ne:cur.txid},__s_.__s_c_time:{$gt:cur.time},__s_.__s_u_txid:-1l}
     *        }
     * ]
     *
     * @param query
     */
    private void findCommonDataQuery_OnSidistranUpdate(DBObject query){
        long txid = MongoTanscationManager.txid_AtThisContext();
        long time = MongoTanscationManager.current().getTx_time();

        DBObject con = new BasicDBObject(stat_f, Values.COMMITED_STAT)
                    .append(txid_f, new BasicDBObject(QueryOperators.LT, txid))
                    .append(u_by_f, new BasicDBObject(QueryOperators.NE, txid))
                    .append(db_time_f, new BasicDBObject(QueryOperators.GT, time))
                    .append(u_by_f, -1l);

        queryAdapt(query, con);
    }


    //主要的逻辑点在于：query的第一层，可能存在一个and，也可能不存在。
    private void queryAdapt(DBObject query, DBObject con){
        //处理ID的映射关系
        sidistranQueryIDProcess(query);

        //这里修改条件，只看query这个json的【最外层】，其他里面不管是否有and，都与此无关。
        if(query.containsField("$and")){
            Object and =  query.get("$and");
            try {
                Object[] con_in_and = (Object[]) and;
                //看看这个and里面是否已经包含该条件
                int i = Arrays.binarySearch(con_in_and, con, conditin_in_and_Finder);
                if (i < 0) {
                    //不存在,则在and里面加一个就行了。
                    //否则就说明已经有了
                    con_in_and = ArrayUtils.add(con_in_and, con);
                    query.put("$and", con_in_and);
                }
            }catch (ClassCastException e){
                if(and instanceof BasicDBList){
                    boolean find = false;
                    for(Iterator itr = ((BasicDBList)and).iterator();
                            itr.hasNext();){
                        Object o = itr.next();
                        if((o instanceof DBObject)&&o.equals(con)) {
                            find = true;
                            break;
                        }
                    }
                    if(!find){
                        ((BasicDBList)and).add(con);
                    }
                }
            }
        }else{
            //没得and,那就加一个and到条件冲
            query.put("$and", new DBObject[]{con});
        }
    }

    //queryAdapt的辅助处理。主要是处理or。对于每一个or因子，需要增加一个and
    @Deprecated
    private void updateOr(Object[] or, BasicDBObject con){
        for(Object o1 : or){
             if (o1 instanceof Map){
                Map o = (Map)o1;
                if(o.containsKey("$and")){
                    Object[] and = (Object[])o.get("$and");
                    int i = Arrays.binarySearch(and, con, conditin_in_and_Finder);
                    if(i<0) {
                        and = ArrayUtils.add(and, con);
                        o.put("$and", and);
                    }
                }else{
                    ((Map)o1).put("$and", new BasicDBObject[]{con});
                }
            }else if(o1 instanceof BSONObject){
                BSONObject o = (BSONObject)o1;
                if(o.containsField("$and")){
                    Object[] and = (Object[])o.get("$and");
                    int i = Arrays.binarySearch(and, con, conditin_in_and_Finder);
                    if(i<0) {
                        and = ArrayUtils.add(and, con);
                        o.put("$and", and);
                    }
                }else{
                    ((BSONObject)o1).put("$and", new BasicDBObject[]{con});
                }
            }else{
                throw new UnsupportedOperationException(o1.getClass()+"之前还没遇到过");
            }
        }
    }

    /**
     * 处理查询返回结果，过滤掉version控制字段
     *
     * 这个方法的作用是：
     * 判断projection中是否指定了“要返回”的字段
     * 如果指定了，就不用再处理了。
     *
     * 如果指定了，但是指定的是“不返回”某些字段
     * 则需要把version控制字段给加入到“不返回”列表中
     *
     * @param projection
     */
    private void projectionAdapt(DBObject projection){
        if(projection.keySet().size()>0){
            //2016-10-17 wx 所有projection都会默认放回_id,因此，这里判断的时候需要把_id给排除
            for(Iterator<String> itr = projection.keySet().iterator();
                itr.hasNext();){
                String key = itr.next();
                if(!key.equals(ID_FIELD_NAME)){
                    Object val = projection.get(key);
                    if(((val instanceof Boolean)&&(Boolean)val)
                        ||((val instanceof Number)&&((Number)val).intValue()==1)){
                        return;
                    }
                }
            }
        }
        
        if(!projection.containsField(stat_f)){
            projection.put(stat_f, false);
        }
        if(!projection.containsField(txid_f)){
            projection.put(txid_f, false);
        }
        if(!projection.containsField(u_by_f)){
            projection.put(u_by_f, false);
        }
        if(!projection.containsField(db_time_f)){
            projection.put(db_time_f, false);
        }
        if(!projection.containsField(Fields.UNIQUE_)){
            projection.put(Fields.UNIQUE_, false);
        }
    }

    /**
     * 处理aggregate的pipeline
     */
    private void pipeLineAdapt(List<? extends DBObject> pipeline, boolean sidistran){
        //由于pipeline是一个顺序管道过滤。
        //因此看看最开始是否有一个match。如果没有，则增加。如果有，则修改
        if(pipeline==null) throw new IllegalArgumentException("pipeline不能为空");

        DBObject match = pipeline.get(0);
        if(match.get("$match")!=null){
            //说明第一个就是match
            DBObject query = (DBObject)match.get("$match");
            if(sidistran){
                sidistranQueryAdapt(query);
            }else{
                commonQueryAdapt(query);
            }
        }else{
            //第一个不是match
            DBObject query = new BasicDBObject();
            if(sidistran){
                sidistranQueryAdapt(query);
            }else{
                commonQueryAdapt(query);
            }
            query = new BasicDBObject("$match", query);

            //反射执行 pipeline.add(0, query);
            //由于list是一个? extends A泛型，属于只读泛型，list写入的时候，？不确定，而List是一个强类型数组
            //因此add编译不过。
            try {
                MethodUtils.invokeMethod(pipeline, "add", new Object[]{0, query});
            }catch (Exception e){
                if(e instanceof RuntimeException){
                    throw (RuntimeException)e;
                }
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * 在Sidistran处理的collection中，
     * _id在写入的时候都被转换成了__s_._id，在查询返回的_id时候，都使用__s_._id来返回
     *
     * 因此，查询的时候，需要处理这个条件
     * @param query
     */
    private void sidistranQueryIDProcess(DBObject query){
        if(query.containsField("_id")){
            Object o = query.removeField("_id");
            query.put(id_f, o);
        }

        Object subQueries = query.get("$and");
        if(subQueries!=null) {
            if (subQueries instanceof Object[]) {
                if (((Object[])subQueries).length > 0) {
                    for (Object subQuery : ((Object[])subQueries)) {
                        sidistranQueryIDProcess((DBObject) subQuery);
                    }
                }
            }else if(subQueries instanceof BasicDBList){
                for(Iterator itr = ((BasicDBList) subQueries).iterator();
                    itr.hasNext();){
                    sidistranQueryIDProcess((DBObject)itr.next());
                }
            }
        }

        subQueries = query.get("$or");
        if(subQueries!=null) {
            if (subQueries instanceof Object[]) {
                if (((Object[])subQueries).length > 0) {
                    for (Object subQuery : ((Object[])subQueries)) {
                        sidistranQueryIDProcess((DBObject) subQuery);
                    }
                }
            }else if(subQueries instanceof BasicDBList){
                for(Iterator itr = ((BasicDBList) subQueries).iterator();
                    itr.hasNext();){
                    Object o = itr.next();
                    if(o instanceof DBObject) {
                        sidistranQueryIDProcess((DBObject) o);
                    }
                }
            }
        }
    }


    /**
     * 对update进行处理。
     * 在事务环境下，update的对象是那些：1、snapshot；2、insert的
     *
     * snapshot数据设置了Field.UNIQUE_字段为1——通过{@see this.refreshUniqueFields()}可以知道，这个字段是用来
     * 把已有的唯一键区分开的字段——这样一来，snapshot数据不会和原始数据出现唯一键冲突
     *
     * insert的数据，没有设置Field.UNIQUE_字段，如果出现唯一键冲突，那么直接就回滚了，这是合理的。
     *
     * 因此，现在update所面对的数据，要么是snapshot，要么是insert的无冲突数据。
     *
     * 在update的时候，可能会set某些字段，而这些字段恰好是唯一键对应的字段，
     * 那么，这里就需要处理：
     *      如果update包含了唯一键字段，那么，就需要unset Field.UNIQUE_字段
     * 这样的话，如果set的字段和那些原始数据的唯一键字段冲突，就会报错。
     *
     * 需要处理的update内容有这些：
     * {@see https://docs.mongodb.org/manual/reference/operator/update/#id1}
     *
     * $inc	        Increments the value of the field by the specified amount.
     * $mul	        Multiplies the value of the field by the specified amount.
     * $rename	    Renames a field.
     * $set	        Sets the value of a field in a document.
     * $unset	    Removes the specified field from a document.
     * $min	        Only updates the field if the specified value is less than the existing field value.
     * $max         Only updates the field if the specified value is greater than the existing field value.
     * $currentDate	Sets the value of a field to current date, either as a Date or a Timestamp.
     * 
     * $addToSet	Adds elements to an array only if they do not already exist in the set.
     * $pop	        Removes the first or last item of an array.
     * $pullAll	    Removes all matching values from an array.
     * $pull	    Removes all array elements that match a specified query.
     * $pushAll	    Deprecated. Adds several items to an array.
     * $push        Adds an item to an array.
     *
     * 要么，就是直接的替换式update
     *
     * @param update
     */
    private void uniqueIndexFieldProcess(DBObject update, boolean sidistran){
        if(logger.isDebugEnabled()){
            logger.debug("【sidistran_mongo_dbcollection】事务环境下update，检测update是否需要处理唯一键字段\n, 当前update为:" + update);
        }

        boolean updateOP = false;
        //要么是update操作符
        out:for(Iterator<String> itr = update.keySet().iterator();
            itr.hasNext();){
            String key = itr.next();
            if(ops.contains(key)){
                updateOP = true;
                DBObject updateOPVal = (DBObject)update.get(key);
                for(Iterator<String> itr_ = updateOPVal.keySet().iterator();
                    itr_.hasNext();){
                    String field = itr_.next();
                    for(Iterator<List<String>> itr_1 = uniqueIndiceFields.values().iterator();
                        itr_1.hasNext();) {
                        List<String> fieldsInIndex = itr_1.next();
                        if (fieldsInIndex.contains(field)) {
                            DBObject unset = (DBObject) update.get("$unset");
                            if (unset == null) {
                                update.put("$unset", new BasicDBObject(Fields.UNIQUE_, ""));
                            } else {
                                unset.put(Fields.UNIQUE_, "");
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("SidistranDBCollection在事务环境下update，检测到update操作符需要处理唯一键字段:" + field + "\n修改后update为:" + update);
                            }
                            break out;
                        }
                    }
                }
            }
        }

        //这里开始补充字段。
        //对于upsert=true的update，走到这里
        //在事务环境下，状态都应该是new。非事务环境下，保持提交状态
        //同时，如果update的数据是传入了id的，那么需要继承这个ID
        if(!updateOP){
            //要么是全替换式的Bson
            //这种情况，是全替换document，因此需要补录控制字段。
            DBObject body;
            if(sidistran) {
                body = new BasicDBObject();
                long txid = MongoTanscationManager.txid_AtThisContext();
                body.put(Fields.TXID_FILED_NAME, txid);
                body.put(Fields.STAT_FILED_NAME, Values.INSERT_NEW_STAT);
            }else{
                body = new BasicDBObject();
                body.put(Fields.STAT_FILED_NAME, Values.COMMITED_STAT);
                body.put(Fields.GARBAGE_TIME_NAME, Long.MAX_VALUE);
                body.put(Fields.TXID_FILED_NAME, -1l);
                body.put(Fields.UPDATEBY_TXID_NAME, -1l);
            }
            update.put(Fields.ADDITIONAL_BODY, body);
        }else{
            //要么就是操作符处理
            DBObject $set = null;
            for(Iterator<String> itr = update.keySet().iterator();
                itr.hasNext();) {
                String key = itr.next();
                if (key.equals("$set")) {
                    $set = (DBObject)update.get(key);
                    break;
                }
            }
            if($set==null){
                $set = new BasicDBObject();
                update.put("$set", $set);
            }

            if(sidistran) {
                long txid = MongoTanscationManager.txid_AtThisContext();
                $set.put(txid_f, txid);
                $set.put(stat_f, Values.INSERT_NEW_STAT);
            }else{
                $set.put(txid_f, -1l);
                $set.put(stat_f, Values.COMMITED_STAT);
                $set.put(db_time_f, Long.MAX_VALUE);
                $set.put(u_by_f, -1l);
            }
        }
    }

    /**
     * 对原始数据(stat=2)进行update处理
     * 1.增加当前事务标识，标识事务在update这个数据
     * 2.增加历史处理记录
     */
    private DBObject getOriDataUpdateObj(){
        long txid = MongoTanscationManager.txid_AtThisContext();
        DBObject update = new BasicDBObject();

        update.put("$set", new BasicDBObject(u_by_f, txid));

        return update;
    }

    /**
     */
    private DBObject getRemoveUpdateObj(){
        BasicDBObject update = new BasicDBObject("$set",
            new BasicDBObject(stat_f, Values.NEED_TO_REMOVE));

        return update;
    }

    /**
     * 在失败的时候，释放当前事务占用的资源
     * 回滚的时候，这部分数据就不需要处理了
     */
    private void proRollback(DBEncoder dbEncoder){
        long txid = MongoTanscationManager.txid_AtThisContext();
        //释放所有的资源
        //1.将占用数据，解除
        super.update(new BasicDBObject(u_by_f, txid),
            new BasicDBObject("$set", new BasicDBObject(u_by_f, -1l)),
            false, true, WriteConcern.ACKNOWLEDGED, dbEncoder
        );
        //2.标记所有的临时数据为需要被删除
        super.update(
            new BasicDBObject(txid_f, txid).append(stat_f, new BasicDBObject("$ne", Values.COMMITED_STAT)),
            new BasicDBObject(
                "$set",
                new BasicDBObject(stat_f, Values.NEED_TO_REMOVE)
                .append(Fields.UNIQUE_, UUID.randomUUID().toString()) //避免出现垃圾数据的唯一键冲突
            ),
            false, true, WriteConcern.ACKNOWLEDGED, dbEncoder
        );
    }

    //看看在一组and的条件中，是否已经包含o2代表的条件对象
    private static Comparator conditin_in_and_Finder = new  Comparator<Object>() {
        @Override
        public int compare(Object o1, Object o2) {
            if((o1 instanceof BasicDBObject)&&(o2 instanceof BasicDBObject)
                &&o1.equals(o2)) {
                return 0;
            }
            return -1;
        }
    };


    private static enum UpdateType{
        REMOVE,
        UPDATE
    }
    //endregion
}
