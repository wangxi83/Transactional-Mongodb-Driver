package com.sobey.jcg.sobeyhive.sidistran.commons;

import java.lang.reflect.Method;

/**
 * Created by WX on 2015/12/25.
 *
 * 字符串游戏
 */
public class MethodNameUtil {
    public static String toGenericName(Method m){
        String genericName = m.toGenericString();
        String name = m.getName();
        genericName = genericName.substring(genericName.indexOf(name), genericName.length());
        return genericName;
    }
}
