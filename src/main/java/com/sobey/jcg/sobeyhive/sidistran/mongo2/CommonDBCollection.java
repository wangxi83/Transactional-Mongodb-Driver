package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import com.mongodb.DB;
import com.mongodb.DBCollection;

/**
 * Created by WX on 2015/12/29.
 *
 * 这是一个完全普通的Coollection,当properties配置中没有这个collection时返回
 */
public class CommonDBCollection extends DBCollection {
    CommonDBCollection(DB database, String name) {
        super(database, name);
    }
}
