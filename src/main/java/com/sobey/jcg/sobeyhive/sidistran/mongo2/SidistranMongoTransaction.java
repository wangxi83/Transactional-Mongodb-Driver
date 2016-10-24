//package com.sobey.jcg.sobeyhive.sidistran.mongo2;
//
//import org.apache.commons.lang.exception.ExceptionUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.mongodb.BasicDBList;
//import com.mongodb.BasicDBObject;
//import com.mongodb.DBCollection;
//import com.mongodb.DBObject;
//import com.mongodb.MongoClient;
//import com.mongodb.WriteConcern;
//import com.sobey.jcg.sobeyhive.sidistran.ITxManager;
//import com.sobey.jcg.sobeyhive.sidistran.commons.InvokerFactory;
//import com.sobey.jcg.sobeyhive.sidistran.registry.Reference;
//
///**
// * Created by WX on 2016/1/20.
// *
// * 分布式事务对象
// */
//class SidistranMongoTransaction extends MongoTransaction{
//    private Logger logger = LoggerFactory.getLogger(MongoTanscationManager.class);
//
//    private String managerURL;
//    private String pid;
//    private DBCollection txCollection;
//    private String sidistranTxID;
//
//    SidistranMongoTransaction(String sidistranTxID, String pid, MongoClient real_client, String managerURL){
//        super(real_client);
//        this.sidistranTxID = sidistranTxID;
//        this.managerURL = managerURL;
//        this.pid = pid;
//        this.txCollection = this.real_client.getDB(Constants.TRANSACTION_DB)
//            .getCollection(Constants.TRANSACTION_TX_CLT);
//    }
//
//    @Override
//    protected void onError(Throwable e) {
//        logger.error("【sidistran_mongo_distributetransaction】onError: "+ ExceptionUtils.getRootCauseMessage(e), e);
//        this.rollback();
//        try{
//            ITxManager txManager = InvokerFactory.getInvoker(this.managerURL, ITxManager.class);
//            txManager.reportParticipantError(pid, ExceptionUtils.getFullStackTrace(e));
//            txManager.rollback(this.sidistranTxID, false);
//        }catch (Exception ex){
//            ex.printStackTrace();
//        }
//
//    }
//
//    @Override
//    protected void addTransactionTargetIfAbsent(DBCollection collection) {
//        //将事务目标写入当前事务缓存
//        DBObject query = new BasicDBObject("sidistranTxID", sidistranTxID);
//        this.txCollection.update(
//            query,
//            new BasicDBObject("$push", new BasicDBObject("txtargets", collection.getName() + "@" + collection.getDB().getName())),
//            false,
//            false,
//            WriteConcern.ACKNOWLEDGED
//        );
//    }
//
//    @Override
//    public void commit() {
//        //不做事情，等待分布式提交
//        if(logger.isDebugEnabled()){
//            logger.debug("【sidistran_mongo_distributetransaction】逻辑提交，等待分布式提交. tx="+this.toString());
//        }
//    }
//
//    @Override
//    public void rollback() {
//        //不做事情，等待分布式回滚
//        if(logger.isDebugEnabled()){
//            logger.debug("【sidistran_mongo_distributetransaction】逻辑回滚，等待分布式回滚. tx="+this.toString());
//        }
//    }
//
//    /**
//     * 接收网络调用
//     * @param references
//     */
//    void commit(Reference[] references) {
//        if(logger.isDebugEnabled()){
//            logger.debug("【sidistran_mongo_distributetransaction】执行分布式提交. tx="+this.toString());
//        }
//        retriveTarget();
//        if(logger.isDebugEnabled()){
//            logger.debug("【sidistran_mongo_distributetransaction】执行分布式提交，获取事务目标完成. tx="+this.toString());
//        }
//        super.commit();
//    }
//
//    /**
//     * 接受网路哦嗲用
//     * @param references
//     */
//    void rollback(Reference[] references) {
//        if(logger.isDebugEnabled()){
//            logger.debug("【sidistran_mongo_distributetransaction】执行分布式回滚. tx="+this.toString());
//        }
//        retriveTarget();
//        if(logger.isDebugEnabled()){
//            logger.debug("【sidistran_mongo_distributetransaction】执行分布式回滚，获取事务目标完成. tx="+this.toString());
//        }
//        super.rollback();
//    }
//
//    @SuppressWarnings("deprecation")
//    void retriveTarget(){
//        //从当前事务对象中获取哪些表需要处理
//        DBObject query = new BasicDBObject("sidistranTxID", sidistranTxID);
//        DBObject result = this.txCollection.findOne(query);
//        if(result!=null){
//            this.setTxid((Long)result.get("_id"));
//            BasicDBList targets = (BasicDBList)result.get("txtargets");
//            if(targets!=null&&!targets.isEmpty()){
//                for(int i=0; i<targets.size(); i++){
//                    String target = (String)targets.get(i);
//                    String[] cl_at_DB = target.split("@");
//                    DBCollection collection = this.real_client.getDB(cl_at_DB[1]).getCollection(cl_at_DB[0]);
//                    super.addTransactionTargetIfAbsent(collection);
//                }
//            }
//        }
//    }
//
//    @Override
//    public String toString() {
//        return "{Distrbuted=("+sidistranTxID+"), tixd=("+txid+"), dbtime=("+tx_time+")}";
//    }
//}
