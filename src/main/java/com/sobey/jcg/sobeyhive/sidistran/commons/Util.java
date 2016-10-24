package com.sobey.jcg.sobeyhive.sidistran.commons;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by WX on 2015/12/29.
 */
public final class Util {
    /**
     * 增加一个方法
     * @author wx
     * @return
     */
    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        }
        catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = Util.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() returning null indicates the bootstrap ClassLoader
                try {
                    cl = ClassLoader.getSystemClassLoader();
                }
                catch (Throwable ex) {
                    // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
                }
            }
        }
        return cl;
    }

    /**
     * 增加一个方法
     * @author wx
     * @return
     */
    public static List<Class> getAllInterfaces(Class c){
        List<Class> interfaces = new ArrayList<>();
        Class[] ifs = c.getInterfaces();
        if(ifs!=null&&ifs.length>0){
            for(Class clz : ifs){
                interfaces.add(clz);
                List<Class> iInterfaces = getAllInterfaces(clz);
                if(interfaces!=null&&interfaces.size()>0){
                    for(Class iinterface : iInterfaces) {
                        if(!interfaces.contains(iinterface)) {
                            interfaces.add(iinterface);
                        }
                    }
                }
            }
        }
        //获取超类的接口
        Class sperClass = c.getSuperclass();
        if(sperClass!=null&&!sperClass.equals(Object.class)){
            List<Class> superInterfaces = getAllInterfaces(sperClass);
            if(superInterfaces!=null&&superInterfaces.size()>0){
                for(Class sinterface : superInterfaces) {
                    if(!interfaces.contains(sinterface)) {
                        interfaces.add(sinterface);
                    }
                }
            }
        }

        return interfaces;
    }

    /**
     * 增加一个方法
     * @author wx
     * @return
     */
    public static List<Class> getClassesInPackage(Package p) throws ClassNotFoundException{
        String pckgname = p.getName();
        ClassLoader cld = getDefaultClassLoader();
        if (cld == null)
            throw new UnknownError("Can't get class loader.");
        String path = File.separator + pckgname.replaceAll("\\.", "\\"+File.separator);
        URL resource = cld.getResource(path);
        if (resource != null) {
            try {
                File directory = new File(URLDecoder.decode(resource.getFile(), "UTF-8"));
                List<Class> arrayList = new ArrayList<>();
                if (directory.exists()) {
                    String[] files = directory.list();
                    for (int i = 0; i < files.length; i++) {
                        if (files[i].endsWith(".class")) {
                            arrayList.add(Class.forName(pckgname + '.'
                                + files[i].substring(0, files[i].length() - 6)));
                        }
                    }
                }
                return arrayList;
            }catch (UnsupportedEncodingException e){

            }
        }
        return null;
    }

    public static final String LOCALHOST = "127.0.0.1";
    private static volatile InetAddress LOCAL_ADDRESS = null;

    public static String getLocalHost(){
        InetAddress address = getLocalAddress();
        return address == null ? LOCALHOST : address.getHostAddress();
    }

    public static InetAddress getLocalAddress() {
        if (LOCAL_ADDRESS != null)
            return LOCAL_ADDRESS;
        InetAddress localAddress = getLocalAddress0();
        LOCAL_ADDRESS = localAddress;
        return localAddress;
    }

    private static InetAddress getLocalAddress0() {
        InetAddress localAddress = null;
        try {
            localAddress = InetAddress.getLocalHost();
            if (isValidAddress(localAddress)) {
                return localAddress;
            }
        } catch (Throwable e) {
        }
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    try {
                        NetworkInterface network = interfaces.nextElement();
                        Enumeration<InetAddress> addresses = network.getInetAddresses();
                        if (addresses != null) {
                            while (addresses.hasMoreElements()) {
                                try {
                                    InetAddress address = addresses.nextElement();
                                    if (isValidAddress(address)) {
                                        return address;
                                    }
                                } catch (Throwable e) {
                                }
                            }
                        }
                    } catch (Throwable e) {
                    }
                }
            }
        } catch (Throwable e) {
        }
        return localAddress;
    }

    private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");
    public static final String ANYHOST = "0.0.0.0";
    private static boolean isValidAddress(InetAddress address) {
        if (address == null || address.isLoopbackAddress())
            return false;
        String name = address.getHostAddress();
        return (name != null
            && ! ANYHOST.equals(name)
            && ! LOCALHOST.equals(name)
            && IP_PATTERN.matcher(name).matches());
    }
}
