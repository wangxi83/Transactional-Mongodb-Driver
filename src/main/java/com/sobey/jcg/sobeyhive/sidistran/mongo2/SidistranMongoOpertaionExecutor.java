package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.lang.reflect.Field;

import org.apache.commons.lang.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Cluster;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.sobey.jcg.sobeyhive.sidistran.mongo2.ex.SidistranMongoException;

/**
 * Created by WX on 2015/12/29.
 *
 * 这里就是处理的地方
 *
 * Mongo的
 */
class SidistranMongoOpertaionExecutor implements OperationExecutor {
    private SidistranMongoClient mongoClient;
    private Cluster cluster;
    
    private static final int MAX_RETRY_TIMES = 3;       //默认重试3次

    private static Logger logger = LoggerFactory.getLogger(SidistranMongoOpertaionExecutor.class); 
    
    //这个构造函数只会执行一次，并且这个类也只有一个单例实例
    //所以说，这里大胆的进行了反射处理,主要是为了得到Mongo里面的cluster
    //@see com.mongodb.Mongo.getDB(final String dbName)
    SidistranMongoOpertaionExecutor(SidistranMongoClient mongoClient){
        this.mongoClient = mongoClient;
        Field field = FieldUtils.getField(this.mongoClient.getClass(), "cluster", true);
        field.setAccessible(true);
        try {
            cluster = (Cluster)field.get(this.mongoClient);
        }catch (IllegalAccessException e){
            throw new SidistranMongoException(e);
        }
    }

    @Override
    public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
        return execute(operation,readPreference,0);
    }
    
    @Override
    public <T> T execute(final WriteOperation<T> operation) {
       return execute(operation,0);
    }

    private <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference,int retrys) {
        
        if (retrys>0){
            logger.warn("execute ReadOperation retry "+retrys+" times");
            /*做10毫秒的休眠*/
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
        }
        
        ReadBinding binding = getReadBinding(readPreference);
        try {
            return operation.execute(binding);
        }catch (MongoTimeoutException | MongoSocketException e) {
            /*2016-10-12 10:43:56 sunxiang 如果发现是mongo出现网络异常,那么就需要重试*/
            if (retrys >= MAX_RETRY_TIMES){
                logger.error("execute ReadOperation retry "+retrys+" times,and always failed");
                throw e;
            }
            
            return execute(operation,readPreference,++retrys);
        }finally {
            binding.release();
        }
    }
    
    private <T> T execute(final WriteOperation<T> operation,int retrys) {

        if (retrys>0){
            logger.warn("execute WriteOperation retry "+retrys+" times");
            /*做10毫秒的休眠*/
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
        }
        
        WriteBinding binding = getWriteBinding();
        try {
            return operation.execute(binding);
        }catch (MongoTimeoutException | MongoSocketException e) {
            /*2016-10-12 10:43:56 sunxiang 如果发现是mongo出现网络异常,那么就需要重试*/
            if (retrys >= MAX_RETRY_TIMES){
                logger.error("execute ReadOperation retry "+retrys+" times,and always failed");
                throw e;
            }

            return execute(operation,++retrys);
        }finally {
            binding.release();
        }
    }
    
    

    WriteBinding getWriteBinding() {
        return getReadWriteBinding(ReadPreference.primary());
    }

    ReadBinding getReadBinding(final ReadPreference readPreference) {
        return getReadWriteBinding(readPreference);
    }

    private ReadWriteBinding getReadWriteBinding(final ReadPreference readPreference) {
        return new ClusterBinding(cluster, readPreference);
    }
}
