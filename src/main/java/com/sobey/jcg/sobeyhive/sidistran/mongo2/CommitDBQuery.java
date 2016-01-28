package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.Map;

import com.mongodb.BasicDBObject;

/**
 * Created by WX on 2016/1/20.
 */
class CommitDBQuery extends BasicDBObject {
    public CommitDBQuery() {
    }

    public CommitDBQuery(int size) {
        super(size);
    }

    public CommitDBQuery(String key, Object value) {
        super(key, value);
    }

    public CommitDBQuery(Map map) {
        super(map);
    }
}
