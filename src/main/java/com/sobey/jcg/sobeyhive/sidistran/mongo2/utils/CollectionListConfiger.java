package com.sobey.jcg.sobeyhive.sidistran.mongo2.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Pattern;

import com.sobey.jcg.sobeyhive.sidistran.commons.Util;

/**
 * Created by WX on 2016/2/17.
 */
public final class CollectionListConfiger {
//    private static Map<String, List<String>> collections = new HashMap<>();
//    private static List<String> collectionAndReIndex = new ArrayList<>();
//    private static Map<String, String> collectionShardKey = new HashMap<>();

    private static Map<String, List<Pattern>> regx_collections = new HashMap<>();
    private static List<Pattern> regx_collectionAndReIndex = new ArrayList<>();
    private static Map<String, String> regx_collectionShardKey = new HashMap<>();

    private static String symb = "#";

    static{
        new CollectionListConfiger("a");
    }

    private CollectionListConfiger(String a){
        try {
            Properties properties = new Properties();
            properties.load(Util.getDefaultClassLoader().getResourceAsStream("sidistran_collections.properties"));

            for(Iterator<Entry<Object, Object>> itr = properties.entrySet().iterator();
                itr.hasNext();){
                Entry entry = itr.next();
                String pro = entry.getKey().toString();

                if (entry.getValue() != null && entry.getValue().toString().equals("1")) {
                    if (pro.toLowerCase().endsWith("ridx")) {
                        pro = pro.replaceFirst("\\.", symb);
                        String colreidx = pro.substring(0, pro.lastIndexOf('.'));
                        if(colreidx.indexOf('.')!=-1){
                            throw new IllegalArgumentException("不能使用“.”，只能使用*");
                        }
                        colreidx = colreidx.replaceAll("\\*", ".*");
                        regx_collectionAndReIndex.add(Pattern.compile(colreidx.toLowerCase()));
                    } else {
                        String db = pro.substring(0, pro.indexOf('.')).toLowerCase();
                        List<Pattern> cols = regx_collections.get(db);
                        if (cols == null) {
                            cols = new ArrayList<Pattern>();
                            regx_collections.put(db, cols);
                        }

                        String col = pro.substring(pro.indexOf('.')+1, pro.length()).toLowerCase();
                        if(col.indexOf('.')!=-1){
                            throw new IllegalArgumentException("不能使用“.”，只能使用*");
                        }
                        col = col.replaceAll("\\*", ".*");
                        cols.add(Pattern.compile(col));
                    }
                } else if (pro.toLowerCase().endsWith("shardkey")) {
                    pro = pro.replaceFirst("\\.", symb);
                    String col = pro.substring(0, pro.lastIndexOf('.')).toLowerCase();
                    if(col.indexOf('.')!=-1){
                        throw new IllegalArgumentException("不能使用“.”，只能使用*");
                    }
                    col = col.replaceAll("\\*", ".*");
                    regx_collectionShardKey.put(col, entry.getValue().toString());
                }

//                    if (entry.getValue() != null && entry.getValue().toString().equals("1")) {
//                        if (pro.toLowerCase().endsWith("ridx")) {
//                            pro = pro.replaceFirst("\\.", symb);
//                            String colreidx = pro.substring(0, pro.lastIndexOf('.'));
//                            collectionAndReIndex.add(colreidx.toLowerCase());
//                        } else {
//                            String[] db$cols = pro.split("\\.");
//                            List<String> cols = collections.get(db$cols[0].toLowerCase());
//                            if (cols == null) {
//                                cols = new ArrayList<String>();
//                                collections.put(db$cols[0].toLowerCase(), cols);
//                            }
//                            cols.add(
//                                db$cols[0].toLowerCase().equalsIgnoreCase("debugall")
//                                    ? "debugall"
//                                    : db$cols[1].toLowerCase()
//                            );
//                        }
//                    } else if (pro.toLowerCase().endsWith("shardkey")) {
//                        pro = pro.replaceFirst("\\.", symb);
//                        String col = pro.substring(0, pro.lastIndexOf('.')).toLowerCase();
//                        collectionShardKey.put(col, entry.getValue().toString());
//                    }
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    public static boolean needRefreshIndex(String dbName, String collection){
        String key = (dbName+symb+collection).toLowerCase();
        if(sidistranCollection(dbName, collection)){
            for(Pattern pattern: regx_collectionAndReIndex){
                if(pattern.matcher(key).matches()){
                    return true;
                }
            }
        }

        return false;
    }

    public static String getShardKey(String dbName, String collection){
        String key = (dbName+symb+collection).toLowerCase();
        String shardKey = null;
        for(Entry<String, String> entry: regx_collectionShardKey.entrySet()){
            if(key.matches(entry.getKey())){
                shardKey = entry.getValue();
            }
        }
        return shardKey;
    }

    public static boolean sidistranCollection(String dbName, String collection){
        if(dbName==null) return false;
        if(collection==null) return false;

        if(!regx_collections.isEmpty()&&regx_collections.containsKey(dbName.toLowerCase())){
            List<Pattern> patterns = regx_collections.get(dbName.toLowerCase());
            for(Pattern pattern : patterns){
                if(pattern.matcher(collection.toLowerCase()).matches()){
                    return true;
                }
            }
        }

        return false;
    }
}
