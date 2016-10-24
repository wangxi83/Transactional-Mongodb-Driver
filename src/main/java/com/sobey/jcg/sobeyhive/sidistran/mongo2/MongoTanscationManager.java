package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.rmi.RemoteException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.sobey.jcg.sobeyhive.sidistran.ParticipantorCriterion;

/**
 * Created by WX on 2016/1/20.
 *
 * Mongo事务管理器。
 *
 * 可以在Spring中配置或者new出来
 *
 * ------------构造函数说明--------------
 * 1、如果是接入分布式Sidistran环境，则应该使用
 * public MongoTanscationManager(SidistranConfig config)
 *
 * 2、如果是本地事务，则直接使用
 * public MongoTanscationManager()
 * --------------------------------------
 *
 * --------------使用注意----------------
 * 1、此类应该单例使用。
 *
 * 2、一旦开启begin,则必须commit
 *
 * 3、在程序结束的时候应该关闭
 * --------------------------------------
 */
public final class MongoTanscationManager {
    public static enum LocalTxType{
        Local,
        Nested
    }

    private static Logger logger = LoggerFactory.getLogger(MongoTanscationManager.class);

    private static int refCount = 0;
    private boolean globalTransaction; //是否是分布式事务方式开启管理器
    private MongoClient mongoClient;
    private DBCollection txCollection;
    private MonTxIDManager txIDManager;
    private DBTime dbTime;
    private MonTranshCleaner cleaner;

//    //sidistran环境配置
//    private SidistranConfig config;
//    //sidistran参与端发布器
//    private ParticipantorExportor exportor;

    //用于保存本地事务
    private static ThreadLocal<MongoTransaction> transactionLocal = new ThreadLocal<>();
    //用于保存内嵌事务
    private static ThreadLocal<MongoTransaction> nestedLocal = new ThreadLocal<>();

    //字段名
    private static String time_f = "time";

    /**
     * 获取本地事务
     *
     * @return MongoTransaction
     */
    public static MongoTransaction current() {
        return transactionLocal.get();
    }



    /**
     * 获取当前事务上下文中的txid
     *
     * @return
     */
    public static long txid_AtThisContext() {
        return nestedLocal.get()!=null
                ?nestedLocal.get().getTxid()
                :(transactionLocal.get() != null ? transactionLocal.get().getTxid() : -1);
    }

    public static boolean hasTransaction() {
        return nestedLocal.get()!=null
            ||transactionLocal.get()!=null;
    }

    /**
     * 分布式事务管理器
     *
     * @param config
     */
//    public MongoTanscationManager(SidistranConfig config, MongoClient mongoClient) {
//        this(mongoClient);
//        this.config = config;
//        globalTransaction = true;
//        //分布式事务，开启分布式提交入口
////        initialSidistran();
//    }

    /**
     * 本地事务管理器
     */
    @SuppressWarnings("deprecation")
    public MongoTanscationManager(MongoClient mongoClient) {
        refCount++;
        if (refCount > 1) {
            throw new IllegalStateException("实例已经产生，该类只能单例使用");
        }
        globalTransaction = false;
        this.mongoClient = (mongoClient instanceof SidistranMongoClient) ?
            ((SidistranMongoClient) mongoClient).returnOriClient() : mongoClient;

        this.txCollection = this.mongoClient.getDB(Constants.TRANSACTION_DB)
            .getCollection(Constants.TRANSACTION_TX_CLT);

        boolean index_ok = false;
        List<DBObject> indices = this.txCollection.getIndexInfo();
        if(!indices.isEmpty()) {
            for (DBObject dbObject : indices) {
                DBObject key = (DBObject) dbObject.get("key");
                if (key.get(time_f) != null && key.get(time_f).toString().equals("1")) {
                    index_ok = true;
                    break;
                }
            }
        }
        if(!index_ok){
            this.txCollection.createIndex(new BasicDBObject(time_f, 1), new BasicDBObject("background", 1));
        }

        this.txIDManager = MonTxIDManager.getFrom(this.mongoClient);
        this.dbTime = DBTime.getFrom(this.mongoClient);
        this.cleaner = MonTranshCleaner.getFrom(this.mongoClient);

        if(logger.isDebugEnabled()){
            logger.debug("MongoTanscationManager初始化。");
        }
    }

    public void begin() {
        long txid;
        //设置事务上下文
        MongoTransaction transaction = transactionLocal.get();
        if (transaction == null) {
//            if (globalTransaction&&!StringUtils.isEmpty(SidistranMongoAware.getTxID())) {
//                //分布式
//                txid = Long.parseLong(SidistranMongoAware.getTxID());
//                //记录分布式事务给出的ID
//                txIDManager.toggleLastGlobalTxID(txid);
//
//                //生成事务
//                transaction = newSidistranTransactionIfAbsent(SidistranMongoAware.getTxID());
//                transactionLocal.set(transaction);
//                //2016-10-21 wx 消费后立即清除线程变量，否则有多线程隐患
//                //而且，一旦transactionLocal设置了，在SdisitranDBCollection中始终会识别到
//                SidistranMongoAware.clear();
//                if(logger.isDebugEnabled()){
//                    logger.debug("【sidistran_mongo_txmanger】全局事务开始. tx="+transaction);
//                }
//            }else {
                //本地事务.
                beginLocal(LocalTxType.Local);
//            }
        }else {
            if (logger.isDebugEnabled()) {
                logger.debug("【sidistran_mongo_txmanger】当前Manager持有transaction. tx=" + transaction+", 无需开启新事务");
            }
        }
        //2016-10-21 wx 消费后立即清除线程变量，否则有多线程隐患
        //而且，一旦transactionLocal设置了，在SdisitranDBCollection中始终会识别到
        SidistranMongoAware.clear();
    }

    public MongoTransaction beginLocal(LocalTxType localTxType){
        //本地事务.
        long txid = txIDManager.nextTxID();
        //看看是不是参与过分布式事务
        long lastGlobalID = txIDManager.toggleLastGlobalTxID(-1l);
        if(lastGlobalID>0){
            txid = ParticipantorCriterion.TxID_Criterion.localTxIDGen(lastGlobalID, txid);
        }
        MongoTransaction transaction = newTransactionIfAbsent(txid);
        if(logger.isDebugEnabled()){
            logger.debug("【sidistran_mongo_txmanger】本地事务开启. tx="+transaction);
        }
        if(localTxType==LocalTxType.Nested) {
            nestedLocal.set(transaction);
        }else{
            transactionLocal.set(transaction);
        }
        return transaction;
    }

    /**
     * 本地事务提交
     */
    public void commit() {
        try {
            if (current() != null) {
                //如果是本地事务，则直接提交
                //如果是分布式的话，这里不会执行
                current().commit();
            }
        }finally {
            //但是类说明中说了，必须要commit，这里要清除
            String tx = current().toString();
            transactionLocal.remove();
            if(logger.isDebugEnabled()){
                logger.debug("【sidistran_mongo_txmanger】执行提交，移除事务(thread="+Thread.currentThread().getName()
                    +"), tx="+tx+" , 状态："+(transactionLocal.get()==null));
            }
        }
    }

    public void commintNested(){
        try {
            if (nestedLocal.get() != null) {
                nestedLocal.get().commit();
            }
        }finally {
            //但是类说明中说了，必须要commit，这里要清除
            String tx = current().toString();
            nestedLocal.remove();
            if(logger.isDebugEnabled()){
                logger.debug("【sidistran_mongo_txmanger】执行内嵌提交，移除事务(thread="+Thread.currentThread().getName()
                    +"), tx="+tx+" , 状态："+(transactionLocal.get()==null));
            }
        }
    }

//    @Override
//    public void commit(String txid, String pid, Reference[] references) throws RemoteException {
//        //分布式提交
//        //@see begin
//        //@see GlobalSidistranAware
//        //只有在GlobalSidistranAware申明了之后，才是分布式事务
//        //分布式事务需要等待Manager来提交
//        new SidistranMongoTransaction(txid, pid, mongoClient, config.getManagerUrl())
//            .commit(references);
//    }

    /**
     * 本地事务回滚
     */
    public void rollback(){
        try {
            if (current() != null) {
                current().rollback();
            }
        }finally {
            String tx = current().toString();
            transactionLocal.remove();
            if(logger.isDebugEnabled()){
                logger.debug("【sidistran_mongo_txmanger】执行回滚，移除事务(thread="+Thread.currentThread().getName()
                    +"), tx="+tx+" , 状态："+(transactionLocal.get()==null));
            }
        }
    }

    public void rollbackNested(){
        try {
            if (nestedLocal.get() != null) {
                nestedLocal.get().rollback();
            }
        }finally {
            String tx = current().toString();
            nestedLocal.remove();
            if(logger.isDebugEnabled()){
                logger.debug("【sidistran_mongo_txmanger】执行内嵌回滚，移除事务(thread="+Thread.currentThread().getName()
                    +"), tx="+tx+" , 状态："+(transactionLocal.get()==null));
            }
        }
    }

//    @Override
//    public void rollback(String txid, String pid, Reference[] references) throws RemoteException {
//        //分布式提交
//        //@see begin
//        //@see GlobalSidistranAware
//        //只有在GlobalSidistranAware申明了之后，才是分布式事务
//        //分布式事务需要等待Manager来提交
//        new SidistranMongoTransaction(txid, pid, mongoClient, config.getManagerUrl())
//            .rollback(references);
//    }

    /**
     * 必须在关闭的时候调用该方法
     */
    public void close(){
//        if(exportor!=null) {
//            exportor.close();
//        }
        cleaner.close();
    }

    static void transactionOnError(Throwable e){
        try {
            if (current() != null) {
                current().onError(e);
            }
        }finally {
            String tx = current().toString();
            transactionLocal.remove();
            nestedLocal.remove();
            if(logger.isDebugEnabled()){
                logger.debug("【sidistran_mongo_txmanger】transactionOnError，移除事务(thread="+Thread.currentThread().getName()
                    +"), tx="+tx+" , 状态："+(transactionLocal.get()==null)+"&&"+(nestedLocal.get()==null));
            }
        }
    }

//    private void initialSidistran(){
//        if(config==null) throw new IllegalArgumentException("sidistranConfig不能为空");
//        if(config.getReceiverProtocol()== Protocol.CLASS){
//            throw new IllegalArgumentException("不支持Class方式的调用");
//        }
//
//        if(logger.isDebugEnabled()){
//            logger.debug("MongoTanscationManager配置了【Sidistran环境】，开始初始化上下文...");
//        }
//
//        exportor = ParticipantorExportor.newIfAbsent(this, config);
//        exportor.publishIfNot();
//        if(logger.isDebugEnabled()){
//            logger.debug("【Sidistran环境】，发布参与端完成...");
//        }
//
//        if(logger.isDebugEnabled()){
//            logger.debug("MongoTanscationManager，【Sidistran环境】初始化完成");
//        }
//    }

    private MongoTransaction newTransactionIfAbsent(long txid){
        DBObject result = findTx(txid);
        MongoTransaction transaction =  new MongoTransaction(txid, this.mongoClient);
        long time = ((Long)result.get(time_f)).longValue();
        transaction.setTx_time(time);
        return transaction;
    }

//    private MongoTransaction newSidistranTransactionIfAbsent(String sidistranTxID){
//        //对于分布式事务，需要记录本地事务和分布式事务ID
//        DBObject result = findTx(sidistranTxID);
//
//        //分布式，则注册参与端
//        ITxManager txManager = InvokerFactory.getInvoker(config.getManagerUrl(), ITxManager.class);
//        ParticipantReg preg = new ParticipantReg();
//        preg.setType(Type.MONGODB);
//        preg.setSurroundID(result.get("_id") + "@"+ mongoClient.getServerAddressList());
//        if(!StringUtils.isEmpty(config.getZkUrl())) {
//            preg.setWho(new Who(ParticipantorCriterion.AS_ZKService.buildMongoZKService(config.getZkUrl())));
//        }else{
//            preg.setWho(new Who(config.getReceiverProtocol(), config.getRecieverHost(),
//                    config.getReceiverPort(), config.getReceiverName())
//            );
//        }
//        //这一段，保证了，随便几个进程，只有第一个进程作为reciever
//        //因为，type一样，surroundid一样，
//        String pid = txManager.addParticipantIfAbsent(sidistranTxID, preg);
//
//        MongoTransaction transaction = new SidistranMongoTransaction(sidistranTxID, pid, this.mongoClient, config.getManagerUrl());
//        transaction.setTxid(Long.parseLong(result.get("_id").toString()));
//        long time = ((Long)result.get(time_f)).longValue();
//        transaction.setTx_time(time);
//        return transaction;
//    }

    private DBObject findTx(String sidistranTxID){
        DBObject query = new BasicDBObject("sidistranTxID", sidistranTxID);
        long time = dbTime.nextTime();
        DBObject update =
            new BasicDBObject("$inc", new BasicDBObject("_dummy", 1l))
                .append("$setOnInsert", new BasicDBObject(time_f, time));

        DBObject result = this.txCollection.findAndModify(query, null, null, false, update, true, false);
        if(result==null){
            //使用global+local来构造一个本地TxID
            long localTxID = txIDManager.nextTxID();
            localTxID = ParticipantorCriterion.TxID_Criterion.localTxIDGen(Long.parseLong(sidistranTxID), localTxID);

            result = new BasicDBObject("_id", localTxID)
                    .append("sidistranTxID", sidistranTxID).append("_dummy", 1l).append(time_f, time);
            this.txCollection.insert(result);
        }
        return result;
    }

    private DBObject findTx(long txid){
        DBObject query = new BasicDBObject("_id", txid);
        long time = dbTime.nextTime();
        DBObject update =
            new BasicDBObject("$inc", new BasicDBObject("_dummy", 1l))
                .append("$setOnInsert", new BasicDBObject(time_f, time));

        return this.txCollection.findAndModify(query, null, null, false, update, true, true);
    }
}
