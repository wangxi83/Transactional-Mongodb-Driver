package com.sobey.jcg.sobeyhive.sidistran.mongo2;

/**
 * Created by WX on 2016/1/4.
 */
public class GlobalSidistranAware {
    private static ThreadLocal<String> txidlocal = new ThreadLocal<String>();

    private GlobalSidistranAware(){}

    public static void announceTx(String tixd){
        txidlocal.set(tixd);
    }

    public static boolean underSidistran(){
        return txidlocal.get()!=null&&!txidlocal.get().equals("");
    }

    public static String getTxID(){
        return txidlocal.get();
    }

    public static void clear(){
        txidlocal.remove();
    }
}
