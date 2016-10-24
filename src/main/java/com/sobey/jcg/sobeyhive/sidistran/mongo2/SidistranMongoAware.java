package com.sobey.jcg.sobeyhive.sidistran.mongo2;

/**
 * Created by WX on 2016/1/4.
 */
public class SidistranMongoAware {
    private static ThreadLocal<String> txidlocal = new ThreadLocal<String>();

    private SidistranMongoAware(){}

    public static void announceTx(String tixd){
        txidlocal.set(tixd);
    }

    public static String getTxID(){
        return txidlocal.get();
    }

    public static void clear(){
        txidlocal.remove();
    }
}
