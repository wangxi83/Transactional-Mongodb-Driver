//package com.sobey.jcg.sobeyhive.sidistran.mongo2;
//
//import java.rmi.Naming;
//import java.rmi.RemoteException;
//import java.rmi.registry.LocateRegistry;
//import java.rmi.registry.Registry;
//import java.rmi.server.UnicastRemoteObject;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang.exception.ExceptionUtils;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.state.ConnectionState;
//import org.apache.curator.framework.state.ConnectionStateListener;
//
//import com.sobey.jcg.sobeyhive.sidistran.ParticipantorCriterion;
//import com.sobey.jcg.sobeyhive.sidistran.TxParticipantor;
//import com.sobey.jcg.sobeyhive.sidistran.commons.SimpleZKClient;
//import com.sobey.jcg.sobeyhive.sidistran.commons.Util;
//import com.sobey.jcg.sobeyhive.sidistran.enums.Protocol;
//import com.sobey.jcg.sobeyhive.sidistran.registry.Reference;
//
///**
// * Created by WX on 2016/1/18.
// *
// * 发布工作就放在这里完成，以后更换发布方式的话，就改这里
// *
// * 这个类是个发布工厂，因为client有可能产生多个
// *
// * 以url为key，产生实例
// */
//final class ParticipantorExportor {
//    private static ConcurrentMap<String, ParticipantorExportor> instanceCache = new ConcurrentHashMap<>();
//
//    static ParticipantorExportor newIfAbsent(TxParticipantor participantor, SidistranConfig sidistranConfig){
//        String objID = sidistranConfig.toString();
//        ParticipantorExportor instance = instanceCache.get(objID);
//        if(instance!=null){
//            return instance;
//        }
//
//        ParticipantorExportor temp = new ParticipantorExportor(participantor, sidistranConfig);
//        instance = instanceCache.putIfAbsent(objID, temp);
//        if(instance!=null){
//            return instance;
//        }else{
//            return temp;
//        }
//    }
//
//    private TxParticipantor participantor;
//    private SidistranConfig sidistranConfig;
//    private boolean published = false;
//
//    private Registry registry;
//    private RMIAdaptor rmiAdapter;
//    private SimpleZKClient zkClient;
//
//
//    private ParticipantorExportor(TxParticipantor participantor, SidistranConfig sidistranConfig) {
//        this.participantor = participantor;
//        this.sidistranConfig = sidistranConfig;
//        if(!StringUtils.isEmpty(sidistranConfig.getZkUrl())){
//            zkClient = SimpleZKClient.getFromPool(sidistranConfig.getZkUrl());
//        }
//    }
//
//    public void publishIfNot(){
//        //目前只发布了RMI
//        if(!published) {
//            synchronized (this) {
//                if (!published) {
//                    if (sidistranConfig.getReceiverProtocol() == Protocol.RMI) {
//                        boolean ok = publishRMI();
//                        if(ok&&zkClient!=null) {
//                            final String path = ParticipantorCriterion.AS_ZKService.buildMongoPaticipantZKPath(sidistranConfig.getRecieverHost());
//                            final String data = ParticipantorCriterion
//                                            .AS_ZKService
//                                            .buildZKServiceNodeData(
//                                                sidistranConfig.getReceiverProtocol(),
//                                                sidistranConfig.getRecieverHost(),
//                                                sidistranConfig.getReceiverPort()
//                                            );
//
//                            zkClient.deleteNode(path);
//                            zkClient.createNode(path, data, true, false);
//
//                            zkClient.addStateListener(new ConnectionStateListener() {
//                                @Override
//                                public void stateChanged(CuratorFramework client, ConnectionState state) {
//                                    if(state==ConnectionState.RECONNECTED){
//                                        zkClient.deleteNode(path);
//                                        zkClient.createNode(path, data, true, false);
//                                    }
//                                }
//                            });
//                        }
//                    }
//                    published = true;
//                }
//                Runtime.getRuntime().addShutdownHook(new Thread() {
//                    public void run() {
//                        close();
//                    }
//                });
//            }
//        }
//    }
//
//    public void close(){
//        //目前只发布了RMI
//        if(sidistranConfig.getReceiverProtocol()==Protocol.RMI) {
//            closeRMI();
//        }
//
//        if (zkClient!=null){
//            zkClient.close();
//        }
//    }
//
//    private boolean publishRMI(){
//        StringBuilder builder = new StringBuilder(100);
//        builder.append("rmi://")
//            .append(StringUtils.isEmpty(sidistranConfig.getRecieverHost())?
//                Util.getLocalHost():sidistranConfig.getRecieverHost())
//            .append(":")
//            .append(sidistranConfig.getReceiverPort())
//            .append("/")
//            .append(sidistranConfig.getReceiverName());
//        try {
//            registry = LocateRegistry.createRegistry(sidistranConfig.getReceiverPort());
//            RMIAdaptor rmiAdapter = new RMIAdaptor();
//            Naming.rebind(builder.toString(), rmiAdapter);
//            this.rmiAdapter = rmiAdapter;
//            return true;
//        } catch (Exception e) {
//            if(ExceptionUtils.getRootCauseMessage(e).indexOf("already in use")<0){
//                throw new RuntimeException(e);
//            }
//            return false;
//        }
//    }
//
//    private void closeRMI(){
//        try{
//            StringBuilder builder = new StringBuilder(100);
//            builder.append("rmi://")
//                .append(sidistranConfig.getRecieverHost())
//                .append(":")
//                .append(sidistranConfig.getReceiverPort())
//                .append("/")
//                .append(sidistranConfig.getReceiverName());
//            String name = builder.toString();
//            Naming.unbind(name);
//            if(this.rmiAdapter!=null) {
//                UnicastRemoteObject.unexportObject(this.rmiAdapter, true);
//            }
//
//            UnicastRemoteObject.unexportObject(registry, true);
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//    }
//
//
//    /*这里发布的是RMI*/
//    class RMIAdaptor extends UnicastRemoteObject implements TxParticipantor{
//        RMIAdaptor() throws RemoteException{
//            super();
//        }
//
//        @Override
//        public void commit(String txid, String pid, Reference[] references) throws RemoteException {
//            ParticipantorExportor.this.participantor.commit(txid, pid, references);
//        }
//
//        @Override
//        public void rollback(String txid, String pid, Reference[] references) throws RemoteException {
//            ParticipantorExportor.this.participantor.rollback(txid, pid, references);
//        }
//    }
//}
