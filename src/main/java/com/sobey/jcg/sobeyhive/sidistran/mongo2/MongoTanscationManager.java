package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
//import com.sobey.jcg.sobeyhive.sidistran.ITxManager;
//import com.sobey.jcg.sobeyhive.sidistran.TxParticipantor;
//import com.sobey.jcg.sobeyhive.sidistran.commons.InvokerFactory;
//import com.sobey.jcg.sobeyhive.sidistran.enums.Protocol;
//import com.sobey.jcg.sobeyhive.sidistran.enums.Type;
//import com.sobey.jcg.sobeyhive.sidistran.registry.ParticipantReg;
//import com.sobey.jcg.sobeyhive.sidistran.registry.Reference;
//import com.sobey.jcg.sobeyhive.sidistran.registry.Who;

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
    private static int refCount = 0;
    private boolean globalTransaction; //是否是分布式事务方式开启管理器
    private MongoClient mongoClient;
    private DBCollection txCollection;
    private MonTxIDManager txIDManager;
    private DBTime dbTime;
    private MonTranshCleaner cleaner;

    //sidistran环境配置
//    private SidistranConfig config;
    //sidistran参与端发布器
//    private ParticipantorExportor exportor;

    //用于保存本地事务
    private static ThreadLocal<MongoTransaction> transactionLocal = new ThreadLocal<MongoTransaction>();
    //用于其他地方判断当前上下文是否有事务
    private static ThreadLocal<Boolean> transactionBoolLocal = new ThreadLocal<>();

    //字段名
    private static String time_f = "time";

    /**
     * 获取本地事务
     *
     * @return MongoTransaction
     */
    static MongoTransaction current() {
        return transactionLocal.get();
    }

    /**
     * 获取当前事务上下文中的txid
     *
     * @return
     */
    public static long txid_AtThisContext() {
        return transactionLocal.get() != null ? transactionLocal.get().getTxid() : -1;
    }

    public static boolean hasTransaction() {
        return transactionBoolLocal.get() != null && transactionBoolLocal.get().booleanValue();
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
//        initialSidistran();
//    }

    /**
     * 本地事务管理器
     */
    @SuppressWarnings("deprecation")
    public MongoTanscationManager(MongoClient mongoClient) {
        refCount++;
        if (refCount > 1) {
            throw new IllegalStateException("实例已经产生，请单例使用");
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
    }

    public void begin() {
        long txid;
        //设置事务上下文
        MongoTransaction transaction = transactionLocal.get();
        if (transaction == null) {
            if (GlobalSidistranAware.underSidistran() && globalTransaction) {
//                //分布式
//                txid = Long.parseLong(GlobalSidistranAware.getTxID());
//                //刷新本地TXID为最大的全局TXID
//                txIDManager.doMaxIfNot(txid);
//                transaction = newSidistranTransactionIfAbsent(txid);
            } else {
                //本地事务.
                //这个判断，说明，当前的处理不支持嵌套事务
                txid = txIDManager.nextTxID();
                transaction = newTransactionIfAbsent(txid);
                if(logger.isDebugEnabled()){
                    logger.debug("MongoTanscationManager，本地事务开始. txid="+transaction.getTxid());
                }
            }
            transactionLocal.set(transaction);
        }
        transactionBoolLocal.set(new Boolean(true));
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
            transactionLocal.remove();
            transactionBoolLocal.remove();
        }
    }

//    @Override
//    public void commit(String txid, String pid, Reference[] references) throws RemoteException {
//        //分布式提交
//        //@see begin
//        //@see GlobalSidistranAware
//        //只有在GlobalSidistranAware申明了之后，才是分布式事务
//        //分布式事务需要等待Manager来提交
//        new SidistranMongoTransaction(Long.parseLong(txid), pid, mongoClient, config.getManagerUrl())
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
            transactionLocal.remove();
            transactionBoolLocal.remove();
        }
    }

//    @Override
//    public void rollback(String txid, String pid, Reference[] references) throws RemoteException {
//        //分布式提交
//        //@see begin
//        //@see GlobalSidistranAware
//        //只有在GlobalSidistranAware申明了之后，才是分布式事务
//        //分布式事务需要等待Manager来提交
//        new SidistranMongoTransaction(Long.parseLong(txid), pid, mongoClient, config.getManagerUrl())
//            .rollback(references);
//    }

    /**
     * 必须在关闭的时候调用该方法
     */
    public void close(){
        //if(exportor!=null) {
        //    exportor.close();
        //}
        cleaner.close();
    }

//    private void initialSidistran(){
//        if(config==null) throw new IllegalArgumentException("sidistranConfig不能为空");
//        if(config.getReceiverProtocol()== Protocol.CLASS){
//            throw new IllegalArgumentException("不支持Class方式的调用");
//        }
//        if(config.getReceiverProtocol()!=Protocol.SPRING_BEAN
//            &&(config.getReceiverPort()<0
//            || StringUtils.isEmpty(config.getReceiverName())
//            ||config.getManagerUrl().toLowerCase().startsWith("class")
//            ||config.getManagerUrl().toLowerCase().startsWith("spring_bean"))){
//            throw new IllegalArgumentException("SidistranConfig设置协议为"+
//                config.getReceiverProtocol()+"，但是没有设置正确的地址、端口、服务名");
//        }
//
//        exportor = ParticipantorExportor.newIfAbsent(this, config);
//        exportor.publishIfNot();
//    }

    private MongoTransaction newTransactionIfAbsent(long txid){
        DBObject result = findTx(txid);
        MongoTransaction transaction =  new MongoTransaction(txid, this.mongoClient);
        long time = ((Long)result.get(time_f)).longValue();
        transaction.setTx_time(time);
        return transaction;
    }

//    private MongoTransaction newSidistranTransactionIfAbsent(long txid){
//        DBObject result = findTx(txid);
//
//        //分布式，则注册参与端
//        ITxManager txManager = InvokerFactory.getInvoker(config.getManagerUrl(), ITxManager.class);
//        ParticipantReg preg = new ParticipantReg();
//        preg.setType(Type.MONGODB);
//        preg.setSurroundID(txid + "@"+ mongoClient.getServerAddressList());
//        preg.setWho(new Who(config.getReceiverProtocol(), config.getRecieverHost(),
//                config.getReceiverPort(), config.getReceiverName())
//        );
//        //这一段，保证了，随便几个进程，只有第一个进程作为reciever
//        //因为，type一样，surroundid一样，
//        String pid = txManager.addParticipantIfAbsent(txid+"", preg);
//
//        MongoTransaction transaction = new SidistranMongoTransaction(txid, pid, this.mongoClient, config.getManagerUrl());
//        long time = ((Long)result.get("time")).longValue();
//        transaction.setTx_time(time);
//        return transaction;
//    }

    private DBObject findTx(long txid){
        DBObject query = new BasicDBObject("_id", txid);
        long time = dbTime.nextTime();
        DBObject update =
            new BasicDBObject("$inc", new BasicDBObject("_dummy", 1l))
                .append("$setOnInsert", new BasicDBObject(time_f, time));

        DBObject result = this.txCollection.findAndModify(query, null, null, false, update, true, true);
        return result;
    }
}
