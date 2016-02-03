package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import junit.framework.TestCase;

/**
 * Created by WX on 2016/1/28.
 */
public class TestMonTranshCleaner extends TestCase {
    private MongoClient mongoClient;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final ServerAddress serverAddress = new ServerAddress("10.54.54.132", 27017);
        String user = "wangxi";        // the user name
        String database = "test";    // the name of the database in which the user is defined
        char[] password = "19831222".toCharArray();    // the password as a character array
        MongoCredential credential = MongoCredential.createCredential(user,database,password);
        final List<MongoCredential> credentials = new ArrayList<>();
        credentials.add(credential);
        mongoClient = new MongoClient(serverAddress, credentials);
    }

    @Test
    public void test() throws Exception{
        MonTranshCleaner transhCleaner = MonTranshCleaner.getFrom(mongoClient);

        String[] str = new String[]{
            "AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"
        };
        while(true){
            for(int i=0; i<RandomUtils.nextInt(10); i++) {
                String name = str[RandomUtils.nextInt(6)];
                System.out.println(name);
                DBCollection collection = mongoClient.getDB("test").getCollection(name);
                transhCleaner.addToTransh(collection);
            }
            Thread.sleep(1000l);
        }
    }


    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        mongoClient.close();
    }
}
