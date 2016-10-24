package com.sobey.jcg.sobeyhive.sidistran.commons;

/**
 * Created by WX on 2015/12/4.
 */
public final class InvokerUrlDesirializer {
    public static String[] deserializeInvokerUrl(String url){
        String[] result = new String[4];

        StringBuilder sb = new StringBuilder(url);
        int sti = 0;
        int fri = sb.indexOf("://");
        result[0]  = sb.substring(sti, fri);

        sti = fri+3;
        fri = sb.indexOf(":", sti);
        if(fri>0) {
            String str2 = sb.substring(sti, fri);
            result[1] = str2;
        }

        sti = fri>0?fri+1:sti;
        fri = sb.indexOf("/", sti);
        if(fri>0) {
            result[2] = sb.substring(sti, fri);
        }

        sti = fri>0?fri+1:sti;
        result[3] = sb.substring(sti);

        return result;
    }
}
