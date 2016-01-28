package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

/**
 * Created by WX on 2015/12/29.
 *
 * mongodb的驱动代理。
 *
 * 1.
 * Mongo官方驱动的基本调用流程是：
 *
 * new MongoClient()（3.0之前是Mongo，MongoClient继承于Mongo，开辟了一系列新的API，不过到底都一样，重要区别是
 * MongoClient默认开启了WriteConcern为ACKNOWNAGE模式，也就是同步反馈才成功。而new Mongo的默认是忽略，也就是发射后不管）
 *    |
 *    |___这里还是以3.0之前的DB对象为例。因为Spring的data里面，依然采用的是DB方式
 *    |
 * MongoClient产生DB对象，并且传入一个接口实现内部类：OpertaionExecutor
 *    |
 *    |
 * DB对象产生DBCollecton，并且一直传递MongoClient传入的OpertaionExecutor
 *    |
 *    |
 * DBCollecton执行的一系列CRUD，最终是通过OpertaionExecutor来执行
 *    |
 *    |
 * OpertaionExecutor其实真正委派执行的，是ReadOperation和WriteOperation。
 * ReadOperation和WriteOperation的实现类里面，是所有的mongoql的实现，并且维护了Connection
 *
 * 2.从代理结构来说，需要
 *   代理MongoClient以便能产生自己的DB，以便能通过自己的DB产生DBCollecton代理
 *   OpertaionExecutor也需要拦截，以便做其他处理
 *
 * 由于这是一个没有接口的类，很多地方是以构造函数作为入口，不是通过工厂产生，因此这里采用的是继承方式
 */
public class SidistranMongoClient extends MongoClient{
    /**  暂时注释这个，这个可以直接代理到OpertaionExecutor这一层
    @Override
    @SuppressWarnings("deprecation")
    public DB getDB(String dbName) {
        DB db = super.getDB(dbName);
        //强制更换DB的executor为自定义的
        //由于DB是被mongo缓存了的，因此这里只会执行一次
        Field field = FieldUtils.getField(db.getClass(), "executor", true);
        field.setAccessible(true);
        try {
            field.set(db, new SidistranMongoOpertaionExecutor(this));
        }catch (IllegalAccessException e){
            throw new SidistranMongoException(e);
        }
        return new SidistranMongoDB(db).getProxyDB();
    }
    */

    private final ConcurrentMap<String, DB> dbCache = new ConcurrentHashMap();
    @Override//from MongoClient
    @SuppressWarnings("deprecation")
    public DB getDB(String dbName) {
        DB db = dbCache.get(dbName);
        if (db != null) {
            return db;
        }
        db = SidistranMongoDB.getDBProxy(this, dbName);
        DB temp = dbCache.putIfAbsent(dbName, db);
        if (temp != null) {
            return temp;
        }else{
            return db;
        }
    }

    private MongoClient oriClient;
    public MongoClient returnOriClient(){
        return oriClient;
    }

    @Override
    public void close() {
        super.close();
        oriClient.close();
    }

    //IP+port的排序
    /**
    private static Comparator<ServerAddress> comparator = new Comparator<ServerAddress>() {
        @Override
        //o1<o2,-1
        //o1==o2,0
        //o1>o2,1
        public int compare(ServerAddress o1, ServerAddress o2) {
            long o1long = Long.parseLong(o1.getHost().replaceAll("\\.", "")+o1.getPort());
            long o2long = Long.parseLong(o2.getHost().replaceAll("\\.", "")+o2.getPort());
            if(o1long<o2long){
                return -1;
            }else if(o1long==o2long){
                return 0;
            }else{
                return 1;
            }
        }
    };*/

    //region -------------------构造函数继承-------------
    public SidistranMongoClient() {
        oriClient = new MongoClient();
    }

    public SidistranMongoClient(String host) {
        super(host, MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(host, MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(String host, MongoClientOptions options) {
        super(host, MongoClientOptions.builder(options)
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(host, MongoClientOptions.builder(options)
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(String host, int port) {
        super(new ServerAddress(host, port), MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(new ServerAddress(host, port), MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(ServerAddress addr) {
        super(addr, MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(addr, MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(ServerAddress addr, List<MongoCredential> credentialsList) {
        super(addr, credentialsList, MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(addr, credentialsList, MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(ServerAddress addr, MongoClientOptions options) {
        super(addr, MongoClientOptions.builder(options)
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(addr, MongoClientOptions.builder(options)
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(ServerAddress addr, List<MongoCredential> credentialsList, MongoClientOptions options) {
        super(addr, credentialsList, MongoClientOptions.builder(options)
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(addr, credentialsList, MongoClientOptions.builder(options)
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(MongoClientURI uri) {
        super(new MongoClientURI(uri.toString(), MongoClientOptions.builder(uri.getOptions())
            .writeConcern(WriteConcern.ACKNOWLEDGED).readPreference(ReadPreference.primary())));
        oriClient = new MongoClient(new MongoClientURI(uri.toString(), MongoClientOptions.builder(uri.getOptions())
            .writeConcern(WriteConcern.ACKNOWLEDGED).readPreference(ReadPreference.primary())));
    }
    //endregion
}
