package com.sobey.jcg.sobeyhive.sidistran;

import java.text.DecimalFormat;

import com.sobey.jcg.sobeyhive.sidistran.enums.Protocol;

/**
 * Created by WX on 2016/2/29.
 *
 * 规范、约定
 */
public final class ParticipantorCriterion {
    public final static class AS_ZKService{
        public final static String ZKHOME = "/sobeyhive/sidistran";

        public final static String MYSQL_ZK_SERVICE = "MysqlTxPa";

        public final static String MONGO_ZK_SERVICE = "MongoTxPa";

        public static String buildMongoZKService(String zkIP){
            return "zookeeper://"+zkIP+"/"+ AS_ZKService.MONGO_ZK_SERVICE;
        }

        public static String buildMysqlZKService(String zkIP){
            return "zookeeper://"+zkIP+"/"+ AS_ZKService.MYSQL_ZK_SERVICE;
        }

        public static String buildMongoPaticipantZKPath(String paHost){
            return AS_ZKService.ZKHOME+"/"+ AS_ZKService.MONGO_ZK_SERVICE+"/"+paHost.replaceAll("\\.","_");
        }

        public static String buildMysqlPaticipantZKPath(String paHost){
            return AS_ZKService.ZKHOME+"/"+ AS_ZKService.MYSQL_ZK_SERVICE+"/"+paHost.replaceAll("\\.","_");
        }

        public static String buildZKServiceNodeData(Protocol p, String host, int port){
            return p+"://"+host+":"+port;
        }
    }

    /*参与端TXID规范*/
    public final static class TxID_Criterion{
        static DecimalFormat idBuilder = new DecimalFormat("0000000000");
        static String makeTxID(long globalTxID){
            return idBuilder.format(globalTxID);
        }

        static DecimalFormat idBuilder2 = new DecimalFormat("000000000");
        public static long localTxIDGen(long globalTxID, long localTxID){
            return Long.parseLong(globalTxID+idBuilder2.format(localTxID));
        }
    }
}
