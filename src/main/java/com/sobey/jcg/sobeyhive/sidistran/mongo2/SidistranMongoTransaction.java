package com.sobey.jcg.sobeyhive.sidistran.mongo2;

/**
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
//import com.sobey.jcg.sobeyhive.sidistran.ITxManager;
//import com.sobey.jcg.sobeyhive.sidistran.commons.InvokerFactory;
//import com.sobey.jcg.sobeyhive.sidistran.registry.Reference;


//Created by WX on 2016/1/20.

class SidistranMongoTransaction extends MongoTransaction{
    private String managerURL;
    private String pid;

    SidistranMongoTransaction(long txid, String pid, MongoClient client, String managerURL){
        super(txid, client);
        this.managerURL = managerURL;
        this.pid = pid;
    }

    @Override
    protected void commit() {
        //不做事情，等待分布式提交

        //但是，要把target给提交到服务端，到时候会传回来
        Map<String, DBCollection> targets = getTransactionTargets();
        StringBuilder str = new StringBuilder();
        for(Iterator<DBCollection> itr = targets.values().iterator();
            itr.hasNext();){
            DBCollection collection = itr.next();
            str.append(collection.getName()).append("@")
                .append(collection.getDB().getName())
                .append(",");
        }
        if(str.length()>0) {
            ITxManager txManager = InvokerFactory.getInvoker(this.managerURL, ITxManager.class);
            txManager.addParticipantReference(pid,
                new Reference[]{new Reference(UUID.randomUUID().toString(), str.toString())});
        }
    }

    @Override
    protected void rollback() {
        super.rollback();
        //不做事情，等待分布式回滚
        ITxManager txManager = InvokerFactory.getInvoker(this.managerURL, ITxManager.class);
        String host ="";
        try {
            host = InetAddress.getByName("localhost").getHostAddress();
        }catch (Exception e){}
        txManager.reportParticipantError(this.pid,
            this.pid + "已经回滚，thread=[" + Thread.currentThread().getName() + "]@" +host );
    }

   
    void commit(Reference[] references) {
        retriveTarget(references);
        super.commit();
    }


    void rollback(Reference[] references) {
        retriveTarget(references);
        super.rollback();
    }

    @SuppressWarnings("deprecation")
    private void retriveTarget(Reference[] references){
        if(references!=null){
            for(Reference reference : references){
                String msg = (String)reference.getValue();
                String[] strs = msg.split(",");
                for(String str: strs){
                    if(!str.isEmpty()) {
                        String[] cls = str.split("@");
                        DBCollection target = this.mongoClient.getDB(cls[1])
                            .getCollection(cls[0]);
                        this.addTransactionTargetIfAbsent(target);
                    }
                }

            }
        }
    }
}*/
