package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.reflect.FieldUtils;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DefaultDBDecoder;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.sobey.jcg.sobeyhive.sidistran.commons.MethodNameUtil;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.ex.SidistranMongoException;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.utils.CollectionListConfiger;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * Created by WX on 2015/12/29.
 *
 * 代理一个DB，用来产生自己的DBCollection
 * 因为没有接口，同时，DB也没有无参构造函数，意味着每次都要super（XX，BB）。
 * 因此这里采用了CGLib来代理，
 */
public class SidistranMongoDB implements MethodInterceptor{
    private SidistranMongoClient mongo;

    private final ConcurrentHashMap<String, DBCollection> collectionCache;

    private SidistranMongoDB(SidistranMongoClient mongo) {
        this.mongo = mongo;
        this.collectionCache = new ConcurrentHashMap<String, DBCollection>();
    }

    //获取一个DB代理。通过Enhancer创建一个实例，然后进行callback拦截该实例的调用
    //同时，对executor这个私有成员进行hack
    //这样，我们就对主要操作类DB，Collection，OpertaionExecutor全部进行了代理
    static DB getDBProxy(SidistranMongoClient mongo, String name){
        SidistranMongoDB sidistranMongoDB = new SidistranMongoDB(mongo);
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(DB.class);
        enhancer.setCallback(sidistranMongoDB);
        DB db = (DB)enhancer.create(new Class[]{Mongo.class, String.class}, new Object[]{mongo, name});
        db.setWriteConcern(WriteConcern.ACKNOWLEDGED);

        //强制拦截executor。
        // ｛狗日的，所以说如果是对一个类进行反射或者反编译，晓得了关键处理，通过cglib+javaassit，真的就可以破解了
        //   java就是这么不安全｝
        //{@see com.mongodb.DB.getCollection(String name)}里面，会把这个executor传递给DBCollection。
        //而最终的执行，都是在executor中执行的
        //ps：本来，这个executor是在MongoClient中产生的。{@see com.mongodb.Mongo.createOperationExecutor()}
        Field field = FieldUtils.getField(db.getClass(), "executor", true);
        field.setAccessible(true);
        try {
            field.set(db, new SidistranMongoOpertaionExecutor(mongo));
        }catch (IllegalAccessException e){
            throw new SidistranMongoException(e);
        }

        return db;
    }

    @Override
    public Object intercept(Object o, Method method, Object[] args,
                            MethodProxy methodProxy) throws Throwable {
        //返回代理后的DBCollection
        if(MethodNameUtil.toGenericName(method).equals(methodName1)
            ||MethodNameUtil.toGenericName(method).equals(methodName2)){
            return getCollection((DB)o, (String)args[0]);
        }

        return methodProxy.invokeSuper(o, args);
    }
    private static String methodName1 = "getCollection(java.lang.String)";
    private static String methodName2 = "getCollectionFromString(java.lang.String)";
    public DBCollection getCollection(DB db, String name) {
        DBCollection collection = collectionCache.get(name);
        if (collection != null) {
            return collection;
        }

        if(CollectionListConfiger.sidistranCollection(db.getName(), name)) {
            //只有配置了的collection，才使用sidistran
            collection = new SidistranDBCollection(db, name);
        }else{
            collection = new CommonDBCollection(db, name);
        }
        if (mongo.getMongoClientOptions().getDbDecoderFactory() != DefaultDBDecoder.FACTORY) {
            collection.setDBDecoderFactory(mongo.getMongoClientOptions().getDbDecoderFactory());
        }
        if (mongo.getMongoClientOptions().getDbEncoderFactory() != DefaultDBEncoder.FACTORY) {
            collection.setDBEncoderFactory(mongo.getMongoClientOptions().getDbEncoderFactory());
        }
        DBCollection old = collectionCache.putIfAbsent(name, collection);
        return old != null ? old : collection;
    }

}
