package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import javax.net.SocketFactory;

import org.bson.codecs.configuration.CodecRegistry;

import com.mongodb.BasicDBObject;
import com.mongodb.DBDecoderFactory;
import com.mongodb.DBEncoderFactory;
import com.mongodb.DefaultDBDecoder;
import com.mongodb.DefaultDBEncoder;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

/**
 * Created by WX on 2016/2/23.
 */
public class SidistranMonProperty {
    private String description;
    private String readPreference = ReadPreference.primary().toString();
    //{w:1, t:null, j:null};
    //或者
    //ACKNOWLEDGED,JOURNALED,MAJORITY,UNACKNOWLEDGED或W1,W2,W3
    private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
    //LOCAL, MAJORITY, DEFAULT
    private ReadConcern readConcern = ReadConcern.DEFAULT;
    private CodecRegistry codecRegistry = MongoClient.getDefaultCodecRegistry();

    private int minConnectionsPerHost;
    private int maxConnectionsPerHost = 100;
    private int threadsAllowedToBlockForConnectionMultiplier = 5;
    private int serverSelectionTimeout = 1000 * 30;
    private int maxWaitTime = 1000 * 60 * 2;
    private int maxConnectionIdleTime;
    private int maxConnectionLifeTime;
    private int connectTimeout = 1000 * 10;
    private int socketTimeout = 0;
    private boolean socketKeepAlive = false;
    private boolean sslEnabled = false;
    private boolean sslInvalidHostNameAllowed = false;
    private boolean alwaysUseMBeans = false;

    private int heartbeatFrequency = 10000;
    private int minHeartbeatFrequency = 500;
    private int heartbeatConnectTimeout = 20000;
    private int heartbeatSocketTimeout = 20000;
    private int localThreshold = 15;

    private String requiredReplicaSetName;
    private DBDecoderFactory dbDecoderFactory = DefaultDBDecoder.FACTORY;
    private DBEncoderFactory dbEncoderFactory = DefaultDBEncoder.FACTORY;
    private SocketFactory socketFactory = SocketFactory.getDefault();
    private boolean cursorFinalizerEnabled = true;


    private MongoClientOptions options;

    public SidistranMonProperty(MongoClientOptions options) {
        this.options = options;
    }
    
    public SidistranMonProperty() {
        
    }

    MongoClientOptions.Builder builder(){
        MongoClientOptions.Builder builder = options==null?MongoClientOptions.builder()
            :MongoClientOptions.builder(options);


        if(this.description!=null) {
            builder.description(this.description);
        }
        if(this.minConnectionsPerHost>0){
            builder.minConnectionsPerHost(this.minConnectionsPerHost);
        }
        if(this.maxConnectionsPerHost>0){
            builder.connectionsPerHost(this.maxConnectionsPerHost);
        }
        if(this.threadsAllowedToBlockForConnectionMultiplier>0){
            builder.connectionsPerHost(this.threadsAllowedToBlockForConnectionMultiplier);
        }
        if(this.serverSelectionTimeout>0){
            builder.serverSelectionTimeout(this.serverSelectionTimeout);
        }
        if(this.maxWaitTime>0){
            builder.maxWaitTime(this.maxWaitTime);
        }
        if(this.maxConnectionIdleTime>0){
            builder.maxConnectionIdleTime(this.maxConnectionIdleTime);
        }
        if(this.maxConnectionLifeTime>0){
            builder.maxConnectionLifeTime(this.maxConnectionLifeTime);
        }
        if(this.connectTimeout> 0){
            builder.connectTimeout(this.connectTimeout);
        }
        if(this.socketTimeout> 0){
            builder.socketTimeout(this.socketTimeout);
        }
        if(this.socketKeepAlive){
            builder.socketKeepAlive(this.socketKeepAlive);
        }
        if(this.sslEnabled){
            builder.sslEnabled(this.sslEnabled);
        }
        if(this.sslInvalidHostNameAllowed){
            builder.sslInvalidHostNameAllowed(this.sslInvalidHostNameAllowed);
        }
        if(this.readPreference!=null){
            builder.readPreference(ReadPreference.valueOf(this.readPreference));
        }
        if(this.writeConcern!=null){
            builder.writeConcern(this.writeConcern);
        }
        if(this.readConcern!=null){
            builder.readConcern(this.readConcern);
        }
        if(this.codecRegistry!=null){
            builder.codecRegistry(this.codecRegistry);
        }
        if(this.socketFactory!=null) {
            builder.socketFactory(this.socketFactory);
        }
        if(this.cursorFinalizerEnabled) {
            builder.cursorFinalizerEnabled(this.cursorFinalizerEnabled);
        }
        if(this.alwaysUseMBeans) {
            builder.alwaysUseMBeans(this.alwaysUseMBeans);
        }
        if(this.dbDecoderFactory!=null) {
            builder.dbDecoderFactory(this.dbDecoderFactory);
        }
        if(this.dbEncoderFactory!=null) {
            builder.dbEncoderFactory(this.dbEncoderFactory);
        }
        if(this.heartbeatFrequency>0) {
            builder.heartbeatFrequency(this.heartbeatFrequency);
        }
        if(this.minHeartbeatFrequency>0) {
            builder.minHeartbeatFrequency(this.minHeartbeatFrequency);
        }
        if(this.heartbeatConnectTimeout>0) {
            builder.heartbeatConnectTimeout(this.heartbeatConnectTimeout);
        }
        if(this.heartbeatSocketTimeout>0) {
            builder.heartbeatSocketTimeout(this.heartbeatSocketTimeout);
        }
        if(this.requiredReplicaSetName!=null) {
            builder.requiredReplicaSetName(this.requiredReplicaSetName);
        }
        if(this.localThreshold>0){
            builder.localThreshold(this.localThreshold);
        }

        return builder;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setReadPreference(String readPreference) {
        this.readPreference = readPreference;
    }

    public void setWriteConcern(String writeConcern) {
        try{
            BasicDBObject bs = (BasicDBObject) JSON.parse(writeConcern);
            Integer w = (Integer)bs.get("w");
            Integer timeout = (Integer)bs.get("t");
            Boolean journal = (Boolean)bs.get("j");
            if(w!=null){
                this.writeConcern = (timeout!=null&&timeout.intValue()>0)?
                                    new WriteConcern(w.intValue(), timeout.intValue()):
                                    new WriteConcern(w.intValue());
            }
            if(journal!=null&&journal.booleanValue()){
                this.writeConcern = this.writeConcern.withJournal(true);
            }
        }catch (com.mongodb.util.JSONParseException e){
            this.writeConcern = WriteConcern.valueOf(writeConcern);
        }
    }

    public void setReadConcern(String readConcern) {
        this.readConcern = new ReadConcern(ReadConcernLevel.fromString(readConcern));
    }

    public void setCodecRegistry(CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    public void setMinConnectionsPerHost(int minConnectionsPerHost) {
        this.minConnectionsPerHost = minConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public void setThreadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
        this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
    }

    public void setServerSelectionTimeout(int serverSelectionTimeout) {
        this.serverSelectionTimeout = serverSelectionTimeout;
    }

    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    public void setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }

    public void setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        this.maxConnectionLifeTime = maxConnectionLifeTime;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
    }

    public void setAlwaysUseMBeans(boolean alwaysUseMBeans) {
        this.alwaysUseMBeans = alwaysUseMBeans;
    }

    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    public void setMinHeartbeatFrequency(int minHeartbeatFrequency) {
        this.minHeartbeatFrequency = minHeartbeatFrequency;
    }

    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {
        this.heartbeatConnectTimeout = heartbeatConnectTimeout;
    }

    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {
        this.heartbeatSocketTimeout = heartbeatSocketTimeout;
    }

    public void setLocalThreshold(int localThreshold) {
        this.localThreshold = localThreshold;
    }

    public void setRequiredReplicaSetName(String requiredReplicaSetName) {
        this.requiredReplicaSetName = requiredReplicaSetName;
    }

    public void setDbDecoderFactory(DBDecoderFactory dbDecoderFactory) {
        this.dbDecoderFactory = dbDecoderFactory;
    }

    public void setDbEncoderFactory(DBEncoderFactory dbEncoderFactory) {
        this.dbEncoderFactory = dbEncoderFactory;
    }

    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    public void setCursorFinalizerEnabled(boolean cursorFinalizerEnabled) {
        this.cursorFinalizerEnabled = cursorFinalizerEnabled;
    }

    public void setOptions(MongoClientOptions options) {
        this.options = options;
    }
}
