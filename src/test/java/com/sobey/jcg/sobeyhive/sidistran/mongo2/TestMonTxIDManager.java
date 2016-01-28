package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import junit.framework.TestCase;

/**
 * Created by WX on 2016/1/26.
 */
public class TestMonTxIDManager extends TestCase {
    private MongoClient mongoClient;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        final ServerAddress serverAddress = new ServerAddress("10.54.54.132", 27017);
        String user = "wangxi";        // the user name
        String database = "test";    // the name of the database in which the user is defined
        char[] password = "asfd".toCharArray();    // the password as a character array
        MongoCredential credential = MongoCredential.createCredential(user,database,password);
        final List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        credentials.add(credential);
        mongoClient = new MongoClient(serverAddress, credentials);
    }

    @Test
    public void testSingleTon() throws Exception{
        final MonTxIDManager txid = MonTxIDManager.getFrom(mongoClient);

        final CountDownLatch countDownLatch = new CountDownLatch(50);
        final boolean[] bools = new boolean[50];
        for(int i=0; i<50; i++) {
            final int idx = i;
            new Thread() {
                public void run() {
                    MonTxIDManager temp = MonTxIDManager.getFrom(mongoClient);
                    bools[idx] = txid.equals(temp);
                    countDownLatch.countDown();
                }
            }.start();
        }
        countDownLatch.await();

        for(int i=0; i<50; i++) {
            assertTrue(bools[i]);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testNextID() throws Exception{
        final MonTxIDManager manager = MonTxIDManager.getFrom(mongoClient);

        final CountDownLatch countDownLatch = new CountDownLatch(50);
        final long[] ids = new long[50];
        for(int i=0; i<50; i++) {
            final int idx = i;
            new Thread() {
                public void run() {
                    ids[idx] = manager.nextTxID();
                    countDownLatch.countDown();
                }
            }.start();
        }
        countDownLatch.await();

        for(int i=0; i<50; i++) {
            System.out.println(ids[i]);
        }
        Arrays.sort(ids);

        DBCollection collection = mongoClient.getDB("transaction").getCollection("public");
        DBObject dbObject = collection.findOne(new BasicDBObject("_id", "ID_Gen"));
        assertEquals(((Long)dbObject.get("seq")).longValue(), ids[49]);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testToggle(){
        long txid = 1l;
        final MonTxIDManager manager = MonTxIDManager.getFrom(mongoClient);

        manager.toggleMaxTxID(txid);

        long txid2 = 11l;

        manager.toggleMaxTxID(txid2);

        long txid3 = 5l;

        long txid4 = manager.toggleMaxTxID(txid3);

        assertTrue(txid3<txid4);

        long txid5 = 100l;
        long txid6 = manager.toggleMaxTxID(txid5);

        assertTrue(txid6==txid5);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        mongoClient.close();
    }
}
