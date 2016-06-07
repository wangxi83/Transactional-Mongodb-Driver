package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
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

    @Override
    public Collection<DB> getUsedDatabases() {
        return dbCache.values();
    }

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

    public SidistranMongoClient(ServerAddress addr, List<SidistranMonCredential> credentials) {
        super(addr, SidistranMonCredential.toList(credentials), MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(addr, SidistranMonCredential.toList(credentials), MongoClientOptions
            .builder().writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(ServerAddress addr, SidistranMonProperty options) {
        super(addr, options.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(addr, options.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(ServerAddress addr, List<SidistranMonCredential> credentials, SidistranMonProperty options) {
        super(addr, SidistranMonCredential.toList(credentials), options.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(addr, SidistranMonCredential.toList(credentials), options.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(List<ServerAddress> seeds) {
        this(seeds, new SidistranMonProperty());
    }

    public SidistranMongoClient(List<ServerAddress> seeds, List<SidistranMonCredential> credentials) {
        super(seeds, SidistranMonCredential.toList(credentials), MongoClientOptions.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(seeds, SidistranMonCredential.toList(credentials), MongoClientOptions.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(List<ServerAddress> seeds, SidistranMonProperty options) {
        super(seeds, options.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(seeds, options.builder()
            .writeConcern(WriteConcern.ACKNOWLEDGED)
            .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(List<ServerAddress> seeds, List<SidistranMonCredential> credentials, SidistranMonProperty options) {
        super(seeds, SidistranMonCredential.toList(credentials),
            options==null
                ?new SidistranMonProperty().builder()
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .readPreference(ReadPreference.primary()).build()
                :options.builder().writeConcern(WriteConcern.ACKNOWLEDGED)
                .readPreference(ReadPreference.primary()).build());
        oriClient = new MongoClient(seeds, SidistranMonCredential.toList(credentials), options==null
                ?new SidistranMonProperty().builder()
                .writeConcern(WriteConcern.ACKNOWLEDGED)
                .readPreference(ReadPreference.primary()).build()
                :options.builder().writeConcern(WriteConcern.ACKNOWLEDGED)
                .readPreference(ReadPreference.primary()).build());
    }

    public SidistranMongoClient(MongoClientURI uri) {
        super(new MongoClientURI(uri.toString(), MongoClientOptions.builder(uri.getOptions())
            .writeConcern(WriteConcern.ACKNOWLEDGED).readPreference(ReadPreference.primary())));
        oriClient = new MongoClient(new MongoClientURI(uri.toString(), MongoClientOptions.builder(uri.getOptions())
            .writeConcern(WriteConcern.ACKNOWLEDGED).readPreference(ReadPreference.primary())));
    }

    //mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
    //[uri]@see https://docs.mongodb.org/manual/reference/connection-string/
    //[options]@see https://docs.mongodb.org/manual/reference/connection-string/#connections-connection-options
    public SidistranMongoClient(String uri, SidistranMonProperty options) {
        this(new MongoClientURI(uri, options!=null?options.builder():new SidistranMonProperty().builder()));
    }

    //mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
    //[uri]@see https://docs.mongodb.org/manual/reference/connection-string/
    //[options]@see https://docs.mongodb.org/manual/reference/connection-string/#connections-connection-options
    public SidistranMongoClient(String replica_sets, String user, String password, String db,
                                SidistranMonProperty options) {
        this(buildURI(replica_sets, user, password, db), options);
    }

    public SidistranMongoClient(String replica_sets, String credentials,  SidistranMonProperty options) {
        this(buildSeeds(replica_sets), buildCredentials(credentials), options);
    }

    //mongo.credentials = sobeyhive:admin@${mongo.db},sobeyhive:admin@transaction
    static List<SidistranMonCredential> buildCredentials(String credentials) {
        if (StringUtils.isEmpty(credentials)) {
            return null;
        }
        try {
            String[] credentialArray = credentials.split(",");
            if(credentialArray.length==0){
                return null;
            }
            List<SidistranMonCredential> credentialList = new ArrayList<>();
            for(String credential: credentialArray){
                String[] item = credential.split("@");
                if(item.length==0){
                    throw new Exception();
                }
                if(StringUtils.isEmpty(item[0])||StringUtils.isEmpty(item[1])){
                    throw new Exception();
                }
                String[] useAndPwd = item[0].split(":");
                if(StringUtils.isEmpty(useAndPwd[0])||StringUtils.isEmpty(useAndPwd[1])){
                    throw new Exception();
                }
                SidistranMonCredential credentialobj = new SidistranMonCredential(useAndPwd[0], useAndPwd[1], item[1]);
                credentialList.add(credentialobj);
            }
            return credentialList;
        }catch (Exception e){
            throw new IllegalArgumentException("credentials格式不对，应该是：[{user}:{pwd}@{db}[,]]的形式");
        }
    }

    //replica_sets = host1[:port1][,host2[:port2]
    static List<ServerAddress> buildSeeds(String replica_sets){
        if (StringUtils.isEmpty(replica_sets)) {
            return null;
        }

        String[] replica_set_Array = replica_sets.split(",");
        if(replica_set_Array.length==0){
            return null;
        }

        List<ServerAddress> seeds = new ArrayList<>();
        for(String replica_set : replica_set_Array){
            String[] item = replica_set.split(":");
            ServerAddress address = new ServerAddress(item[0], Integer.parseInt(item[1]));
            seeds.add(address);
        }
        return seeds;
    }

    //replica_sets = host1[:port1][,host2[:port2]
    static String buildURI(String replica_sets, String user, String password, String db){
        StringBuilder builder = new StringBuilder("mongodb://");
        if(StringUtils.isEmpty(replica_sets)){
            throw new IllegalArgumentException("replica_sets不能為空");
        }
        if(!StringUtils.isEmpty(user)){
            builder.append(user).append(":");
            if(!StringUtils.isEmpty(password)){
                builder.append(password);
            }else{
                throw new IllegalArgumentException("password不能為空");
            }
            builder.append("@");
        }
        builder.append(replica_sets);
        if(!StringUtils.isEmpty(db)){
            builder.append("/").append(db);
        }
        return builder.toString();
    }
    //endregion
}
