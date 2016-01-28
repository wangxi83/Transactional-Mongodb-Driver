package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.MongoReadWriteLock.MonWriteLock;


/**
 * Created by WX on 2016/1/22.
 */
public class TestMongoReadWriteLock {
    private static int count =0;

    public static void main(String[] args) throws Exception{
        ServerAddress serverAddress = new ServerAddress("10.54.54.132", 27017);
        String user = "wangxi";        // the user name
        String database = "test";    // the name of the database in which the user is defined
        char[] password = "asfd".toCharArray();    // the password as a character array
        MongoCredential credential = MongoCredential.createCredential(user, database, password);
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        credentials.add(credential);
        final MongoClient mongo = new MongoClient(serverAddress, credentials);

        Thread wt1 = new Thread("write-1"){
            public void run(){
                for(int i=0; i<23; i++) {
                    MonWriteLock lock = MongoReadWriteLock.getLock(mongo).writeLock();
                    try {
                        System.out.println(this.getName() + ": 写入");
                        lock.lock();
                        count++;
//                        Thread.sleep(RandomUtils.nextInt(300));
                        Thread.sleep(1000l);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
        };
        Thread wt2 = new Thread("write-2"){
            public void run(){
                for(int i=0; i<17; i++) {
                    MonWriteLock lock = MongoReadWriteLock.getLock(mongo).writeLock();
                    try {
                        System.out.println(this.getName() + ": 写入");
                        lock.lock();
                        count++;
                        Thread.sleep(RandomUtils.nextInt(300));
                    }catch (Exception e){}
                    finally {
                        lock.unlock();
                    }
                }
            }
        };
        Thread wt3 = new Thread("write-3"){
            public void run(){
                for(int i=0; i<15; i++) {
                    MonWriteLock lock = MongoReadWriteLock.getLock(mongo).writeLock();
                    try {
                        System.out.println(this.getName() + ": 写入");
                        lock.lock();
                        count++;
                        Thread.sleep(RandomUtils.nextInt(300));
                    }catch (Exception e){}
                    finally {
                        lock.unlock();
                    }
                }
            }
        };
        Thread wt4 = new Thread("write-4"){
            public void run(){
                for(int i=0; i<5; i++) {
                    MonWriteLock lock = MongoReadWriteLock.getLock(mongo).writeLock();
                    try {
                        System.out.println(this.getName() + ": 写入");
                        lock.lock();
                        count++;
                        Thread.sleep(RandomUtils.nextInt(300));
                    }catch (Exception e){}
                    finally {
                        lock.unlock();
                    }
                }
            }
        };


//        for(int i=0; i<5; i++){
//            new Thread("Read-"+i){
//                public void run(){
//                    while (count < 23) {
//                        MonReadLock readLock = MongoReadWriteLock.getLock(mongo).readLock();
//                        try {
//                            readLock.lock();
//                            System.out.println(this.getName() + ": " + count);
//                            Thread.sleep(200);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        } finally {
//                            readLock.unlock();
//                        }
//                    }
//                }
//            }.start();
//        }
//
//        Thread.sleep(100);
//        wt1.start();
//        wt2.start();
//        wt3.start();
//        wt4.start();
        testReentrantAble(mongo);
    }

    public static void testReentrantAble(final MongoClient mongo ){
        final MongoReadWriteLock lock = MongoReadWriteLock.getLock(mongo);
        new Thread("thread-1") {
            public void run(){
                int i=0;
                while(i<30) {
                    try

                    {
                        lock.writeLock().lock();

                        System.out.println(this.getName() + ": write---");
                        lock.readLock().lock();
                        System.out.println(this.getName() + ": read---");
                        lock.readLock().unlock();
                        try {
                            Thread.sleep(RandomUtils.nextInt(1000));
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    } finally

                    {
                        lock.writeLock().unlock();
                        i++;
                    }

                }
            }
        }.start();

        new Thread("thread-2") {
            public void run(){
                int i=0;
                while(i<30) {
                    try

                    {
                        lock.readLock().lock();

                        System.out.println(this.getName() + ": right,.....");
                        lock.writeLock().lock();
                        System.out.println(this.getName() + ": left....");
                        lock.writeLock().unlock();
                        try {
                            Thread.sleep(RandomUtils.nextInt(1000));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } finally

                    {
                        lock.readLock().unlock();
                        i++;
                    }
                }
            }
        }.start();
    }
}


