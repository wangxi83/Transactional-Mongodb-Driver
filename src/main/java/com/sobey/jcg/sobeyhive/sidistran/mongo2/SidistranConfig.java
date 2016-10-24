//package com.sobey.jcg.sobeyhive.sidistran.mongo2;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang.math.RandomUtils;
//
//import com.sobey.jcg.sobeyhive.sidistran.ParticipantorCriterion.AS_ZKService;
//import com.sobey.jcg.sobeyhive.sidistran.commons.Util;
//import com.sobey.jcg.sobeyhive.sidistran.enums.Protocol;
//
///**
// * Created by WX on 2016/1/18.
// */
//public final class SidistranConfig {
//
//    private Protocol receiverProtocol = Protocol.CLASS;//接受manager调用的服务方式
//    private String recieverHost;
//    private int receiverPort = -1; //如果是网路方式，则这个是网路端口
//    private String receiverName = "com.sobey.jcg.sobeyhive.sidistran.mysql.MySQLParticipantor";//默认是类方式
//    private String managerUrl = "class://com.sobey.jcg.sobeyhive.sidistran.TxManager";
//    private String zkUrl;
//
//
//    public SidistranConfig() {
//    }
//
//    public SidistranConfig(Protocol receiverProtocol, String recieverHost, int receiverPort,
//                           String receiverName, String managerUrl) {
//        this(receiverProtocol, recieverHost, receiverPort, receiverName, managerUrl, null);
//    }
//
//    public SidistranConfig(Protocol receiverProtocol, String managerUrl) {
//        this(receiverProtocol, managerUrl, null);
//    }
//
//    public SidistranConfig(Protocol receiverProtocol, String recieverHost, int receiverPort,
//                           String receiverName, String managerUrl, String zkUrl) {
//        this.receiverProtocol = receiverProtocol;
//        this.recieverHost = StringUtils.isEmpty(recieverHost)
//            ?Util.getLocalHost()
//            :recieverHost;
//        this.receiverPort = receiverPort<=0
//            ?(55535+ RandomUtils.nextInt(56535)%(56535-55535+1))
//            :receiverPort;
//        this.receiverName = StringUtils.isEmpty(receiverName)
//            ? AS_ZKService.MONGO_ZK_SERVICE
//            :receiverName;
//        this.managerUrl = managerUrl;
//        if(!StringUtils.isEmpty(zkUrl)&&receiverProtocol!=Protocol.RMI&&receiverProtocol!=Protocol.DUBBO){
//            throw new IllegalArgumentException("useZK只能在网络访问方式下配置，当前访问方式["+receiverProtocol+"].");
//        }
//        this.zkUrl = zkUrl;
//    }
//
//    public SidistranConfig(Protocol receiverProtocol, String managerUrl, String zkUrl) {
//        this(receiverProtocol, null, -1, null, managerUrl, zkUrl);
//    }
//
//
//
//    Protocol getReceiverProtocol() {
//        return receiverProtocol;
//    }
//
//    String getRecieverHost() {
//        return recieverHost;
//    }
//
//    int getReceiverPort() {
//        return receiverPort;
//    }
//
//    String getReceiverName() {
//        return receiverName;
//    }
//
//    String getManagerUrl() {
//        return managerUrl;
//    }
//
//    public String getZkUrl() {
//        return zkUrl;
//    }
//
//    @Override
//    public String toString() {
//        return this.receiverProtocol+recieverHost+receiverPort+receiverName+managerUrl;
//    }
//}
