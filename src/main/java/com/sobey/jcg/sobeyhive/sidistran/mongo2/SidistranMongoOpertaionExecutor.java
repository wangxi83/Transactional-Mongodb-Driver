package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.lang.reflect.Field;

import org.apache.commons.lang.reflect.FieldUtils;

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

    public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
        ReadBinding binding = getReadBinding(readPreference);
        try {
            return operation.execute(binding);
        } finally {
            binding.release();
        }
    }

    public <T> T execute(final WriteOperation<T> operation) {
        WriteBinding binding = getWriteBinding();
        try {
            return operation.execute(binding);
        } finally {
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
