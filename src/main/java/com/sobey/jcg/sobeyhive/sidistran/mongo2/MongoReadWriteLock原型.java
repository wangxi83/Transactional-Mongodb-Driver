package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.math.RandomUtils;

import com.sobey.jcg.sobeyhive.sidistran.mongo2.MongoReadWriteLock原型.Lock.ReadLock;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.MongoReadWriteLock原型.Lock.WriteLock;

/**
 * Created by WX on 2016/1/22.
 */
class MongoReadWriteLock原型 {
    private static int count =0;

    public static void main(String[] args) throws Exception{
        Thread wt1 = new Thread("write-1"){
            public void run(){
                for(int i=0; i<23; i++) {
                    WriteLock lock = Lock.writeLock();
                    try {
                        System.out.println(this.getName() + ": 写入");
                        lock.lock();
                        count++;
//                        Thread.sleep(RandomUtils.nextInt(300));
                        Thread.sleep(1000l);
                    }catch (Exception e){}
                    finally {
                        lock.unlock();
                    }
                }
            }
        };
        Thread wt2 = new Thread("write-2"){
            public void run(){
                for(int i=0; i<17; i++) {
                    WriteLock lock = Lock.writeLock();
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
                    WriteLock lock = Lock.writeLock();
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
                    WriteLock lock = Lock.writeLock();
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


        for(int i=0; i<5; i++){
            new Thread("Read-"+i){
                public void run(){
                    while (count < 60) {
                        ReadLock readLock = Lock.readLock();
                        try {
                            readLock.lock();
                            System.out.println(this.getName() + ": " + count);
                            Thread.sleep(200);
                        } catch (Exception e) {
                        } finally {
                            readLock.unlock();
                        }
                    }
                }
            }.start();
        }

        Thread.sleep(100);
        wt1.start();
        wt2.start();
        wt3.start();
        wt4.start();
    }






    static class Lock{
        private static AtomicBoolean A = new AtomicBoolean(false);
        private static long max = 1000;
        private static AtomicLong B = new AtomicLong(0);

        static boolean aquireA(){
            return A.compareAndSet(false, true);
        }

        static boolean write(){
            return A.get();
        }

        static WriteLock writeLock(){
            return new WriteLock();
        }

        static ReadLock readLock(){
            return new ReadLock();
        }

        static class WriteLock{
            void lock(){
                while(!aquireA()){
                    //设置信号量A，写互斥
                }
                //占领A之后，
                //设置B为Max
                while(B.get()>0){
                    //有读，则不写
                }
            }

            void unlock(){
                B.set(0);
                A.set(false);
            }
        }

        static class ReadLock{
            void lock(){
                while(write()){
                    //只要没有写，就不互斥
                }
                B.getAndIncrement();
            }

            void unlock(){
                B.decrementAndGet();
            }
        }
    }
}
