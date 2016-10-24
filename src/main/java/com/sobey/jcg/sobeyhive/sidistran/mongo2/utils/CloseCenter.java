package com.sobey.jcg.sobeyhive.sidistran.mongo2.utils;

import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by WX on 2016/1/19.
 *
 * 主要是用来处理SidistraMongoCollection中ThreadLocal的关闭
 */
public final class CloseCenter{
    private static ConcurrentMap<String, TimeoutCloseAble> cache = new ConcurrentHashMap<>();

    public static void addCloseAble(TimeoutCloseAble closeAble){
        cache.putIfAbsent(closeAble.getName(), closeAble);
    }

    public static void closeImmediatly(String closeAbleName){
        TimeoutCloseAble closeAble = cache.remove(closeAbleName);
        if(closeAble!=null){
            closeAble.close();
        }
    }

    private static void closeIfAble(){
        for(Iterator<TimeoutCloseAble> itr = cache.values().iterator();
            itr.hasNext();){
            TimeoutCloseAble closeAble = itr.next();
            if(closeAble.reachTimeOut()){
                closeAble.close();
                itr.remove();
            }
        }
    }
    
    private CloseCenter instance = new CloseCenter();//单例

    //单例
    private Timer timer = new Timer();
    private CloseCenter(){
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                closeIfAble();
            }
        }, 100, 20000);
    }
}
