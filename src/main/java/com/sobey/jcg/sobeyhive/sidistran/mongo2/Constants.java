package com.sobey.jcg.sobeyhive.sidistran.mongo2;

/**
 * Created by WX on 2016/1/2.
 */
public interface Constants {
    static interface Fields{
        static final String ADDITIONAL_BODY = "__s_";
        static final String STAT_FILED_NAME = "__s_stat";//状态
        static final String TXID_FILED_NAME = "__s_c_txid";//事务ID
        static final String UPDATEBY_TXID_NAME = "__s_u_txid";
        static final String UPDATE_FROM_NAME = "__s_u_from";
        static final String GARBAGE_TIME_NAME = "__s_g_time";//成为垃圾的时间
    }

    static interface Values{
        static final int INSERT_NEW_STAT = 0; //新增的
//        static final int REMOVE_STAT = -1; //删除的
        static final int COMMITED_STAT = 2; //确认的
//        static final int UPDATE_STAT = 1; //更新的

        static final int NEED_TO_REMOVE = -100;//需要被清理的
    }

    static String TRANSACTION_DB = "transaction";
    static String TRANSACTION_UTIL_CLT = "public";
    static String TRANSACTION_TX_CLT = "active_tx";
}
