package com.sobey.jcg.sobeyhive.sidistran.mongo2.utils;

import java.io.Closeable;

/**
 * Created by WX on 2016/1/19.
 *
 * 辅助类
 */
public final class TimeoutCloseAble {
    private String name;
    private long timeout = 18000000l;
    private long beginTime;
    private Closeable closeable;

    public TimeoutCloseAble(String name, long timeout, Closeable closeable) {
        this.name = name;
        this.timeout = timeout;
        this.closeable = closeable;
        this.beginTime = System.currentTimeMillis();
    }

    public TimeoutCloseAble(String name, Closeable closeable) {
        this.name = name;
        this.closeable = closeable;
    }

    String getName() {
        return name;
    }

    long getTimeout() {
        return timeout;
    }

    void close() {
        try {
            closeable.close();
        }catch (Exception e){
        }
    }

    boolean reachTimeOut(){
        return System.currentTimeMillis()-beginTime>=timeout;
    }
}
