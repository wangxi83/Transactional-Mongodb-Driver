package com.sobey.jcg.sobeyhive.sidistran.mongo2;

/***
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sobey.jcg.sobeyhive.sidistran.TxParticipantor;
import com.sobey.jcg.sobeyhive.sidistran.enums.Protocol;
import com.sobey.jcg.sobeyhive.sidistran.registry.Reference;


final class ParticipantorExportor {
    private static ConcurrentMap<String, ParticipantorExportor> instanceCache = new ConcurrentHashMap<>();

    static ParticipantorExportor newIfAbsent(TxParticipantor participantor, SidistranConfig sidistranConfig){
        String objID = sidistranConfig.toString();
        ParticipantorExportor instance = instanceCache.get(objID);
        if(instance!=null){
            return instance;
        }

        ParticipantorExportor temp = new ParticipantorExportor(participantor, sidistranConfig);
        instance = instanceCache.putIfAbsent(objID, temp);
        if(instance!=null){
            return instance;
        }else{
            return temp;
        }
    }

    private TxParticipantor participantor;
    private SidistranConfig sidistranConfig;
    private boolean published = false;

    private Registry registry;
    private RMIAdaptor rmiAdapter;


    private ParticipantorExportor(TxParticipantor participantor, SidistranConfig sidistranConfig) {
        this.participantor = participantor;
        this.sidistranConfig = sidistranConfig;
    }

    public void publishIfNot(){
        //目前只发布了RMI
        if(!published) {
            synchronized (this) {
                if (!published) {
                    if (sidistranConfig.getReceiverProtocol() == Protocol.RMI) {
                        publishRMI();
                        Runtime.getRuntime().addShutdownHook(new Thread() {
                            public void run() {
                                closeRMI();
                            }
                        });
                    }
                    published = true;
                }
            }
        }
    }

    public void close(){
        //目前只发布了RMI
        if(sidistranConfig.getReceiverProtocol()==Protocol.RMI) {
            closeRMI();
        }
    }

    private void publishRMI(){
        StringBuilder builder = new StringBuilder(100);
        builder.append("rmi://")
            .append(sidistranConfig.getRecieverHost())
            .append(":")
            .append(sidistranConfig.getReceiverPort())
            .append("/")
            .append(sidistranConfig.getReceiverName());
        try {
            registry = LocateRegistry.createRegistry(sidistranConfig.getReceiverPort());
            RMIAdaptor rmiAdapter = new RMIAdaptor();
            Naming.rebind(builder.toString(), rmiAdapter);
            this.rmiAdapter = rmiAdapter;
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        } catch (MalformedURLException e){
            throw new RuntimeException(e);
        }
    }

    private void closeRMI(){
        try{
            StringBuilder builder = new StringBuilder(100);
            builder.append("rmi://")
                .append(sidistranConfig.getRecieverHost())
                .append(":")
                .append(sidistranConfig.getReceiverPort())
                .append("/")
                .append(sidistranConfig.getReceiverName());
            String name = builder.toString();
            Naming.unbind(name);
            if(this.rmiAdapter!=null) {
                UnicastRemoteObject.unexportObject(this.rmiAdapter, true);
            }

            UnicastRemoteObject.unexportObject(registry, true);
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    class RMIAdaptor extends UnicastRemoteObject implements TxParticipantor{
        RMIAdaptor() throws RemoteException{
            super();
        }

        @Override
        public void commit(String txid, String pid, Reference[] references) throws RemoteException {
            ParticipantorExportor.this.participantor.commit(txid, pid, references);
        }

        @Override
        public void rollback(String txid, String pid, Reference[] references) throws RemoteException {
            ParticipantorExportor.this.participantor.rollback(txid, pid, references);
        }
    }
}**/
