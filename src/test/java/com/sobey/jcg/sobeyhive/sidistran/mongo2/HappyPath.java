package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * Created by WX on 2016/1/25.
 */
public class HappyPath {
    public static void main(String[] args) throws Exception{
        final ServerAddress serverAddress = new ServerAddress("10.54.54.132", 27017);
        String user = "wangxi";        // the user name
        String database = "test";    // the name of the database in which the user is defined
        char[] password = "asfd".toCharArray();    // the password as a character array
        MongoCredential credential = MongoCredential.createCredential(user,database,password);
        final List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        credentials.add(credential);

        SidistranMongoClient mongo = new SidistranMongoClient(serverAddress, credentials);
        final MongoTanscationManager tanscationManager = new MongoTanscationManager(mongo);

//        new Thread(){
//            public void run(){
//                SidistranMongoClient mongo = new SidistranMongoClient(serverAddress, credentials);
//                DB db = mongo.getDB("test");
//                DBCollection dbCollection = db.getCollection("student");
//
//                int i=0;
//                while(true){
//                    DBObject dbObject =  new BasicDBObject();
//                    System.out.println("第"+i+"次查询");
//                    DBCursor objects = dbCollection.find(dbObject);
//                    if(objects.count()>0) {
//                        while (objects.hasNext()) {
//                            System.out.println(objects.next());
//                        }
////                        break;
//                    }
//                    i++;
//                    try{Thread.sleep(1000l);}catch (Exception e){}
//                }
//
//            }
//        }.start();



        System.out.println("事务 开始");
        tanscationManager.begin();
        insert(mongo);
        System.out.println("未提交事务");
        find(mongo);
//        if(true){
//            tanscationManager.rollback();
//        }
        tanscationManager.commit();
        System.out.println("事务提交");


//        System.out.println("事务 开始");
//        tanscationManager.begin();
//        find(mongo);
//        update(mongo);
//        System.out.println("未提交事务");
//        find(mongo);
////        if(true){
////            tanscationManager.rollback();
////        }
//        tanscationManager.commit();
//        System.out.println("事务提交");
//
//
//        System.out.println("事务 开始");
//        tanscationManager.begin();
//        find(mongo);
//        remove(mongo);
//        System.out.println("未提交事务");
//        find(mongo);
////        if(true){
////            tanscationManager.rollback();
////        }
//        tanscationManager.commit();
//        System.out.println("事务提交");
        mongo.close();
    }

    public static void find(SidistranMongoClient mongo){
        DB db = mongo.getDB("test");
        DBCollection dbCollection = db.getCollection("student");
        DBObject dbObject =  new BasicDBObject("$and", new BasicDBObject[]{
            new BasicDBObject("bbb", "123")
        });
        DBCursor objects = dbCollection.find(dbObject);
        System.out.println("==========="+MongoTanscationManager.current().getTxid()+"============");
        while(objects.hasNext()){
            System.out.println(objects.next());
        }
        System.out.println("========================================================");
    }

    public static void insert( SidistranMongoClient mongo){
        DB db = mongo.getDB("test");

        DBCollection dbCollection = db.getCollection("student");

        DBObject[] dbObjects = new DBObject[]{
//       BasicDBObjectBuilder.start().add("aaa","222").add("bbb", "123").get(),
            BasicDBObjectBuilder.start().add("aaa","111").add("bbb", "123").get(),
            BasicDBObjectBuilder.start().add("aaa","333").add("bbb", "123").get(),
//       BasicDBObjectBuilder.start().add("aaa","444").add("bbb", "123").get(),
        };
        WriteResult result = dbCollection.insert(dbObjects, WriteConcern.ACKNOWLEDGED);

        System.out.println(result.getN());
    }

    public static void update(SidistranMongoClient mongo){
        DB db = mongo.getDB("test");
        DBCollection dbCollection = db.getCollection("student");

//        DBObject query = new BasicDBObject( new BasicDBObject("aaa", "333"));
        DBObject query = new BasicDBObject("$or", new BasicDBObject[]{
            new BasicDBObject("aaa", "333"), new BasicDBObject("aaa", "111")});

        dbCollection.update(query, new BasicDBObject("$set", new BasicDBObject("ccc","this is")), false, true);
    }

    public static void remove(SidistranMongoClient mongo){
        DB db = mongo.getDB("test");
        DBCollection dbCollection = db.getCollection("student");
        DBObject query = new BasicDBObject("$or", new BasicDBObject[]{
            new BasicDBObject("aaa", "333"), new BasicDBObject("aaa", "111")});

        dbCollection.remove(query);
    }
}
