package com.sobey.jcg.sobeyhive.sidistran.mongo2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoCredential;

/**
 * Created by WX on 2016/2/23.
 */
public class SidistranMonCredential {
    private AuthenticationMechanism mechanism;
    private String userName;
    private String dataBase;
    private char[] password;
    private Map<String, Object> mechanismProperties;

    public SidistranMonCredential() {
    }

    public SidistranMonCredential(String userName, String password, String dataBase) {
        this.userName = userName;
        this.dataBase = dataBase;
        this.password = password.toCharArray();
    }

    public SidistranMonCredential(String userName, String password, String dataBase,
                                  AuthenticationMechanism mechanism, Map<String, Object> mechanismProperties) {
        this.userName = userName;
        this.dataBase = dataBase;
        this.password = password.toCharArray();
        this.mechanism = mechanism;
        this.mechanismProperties = mechanismProperties;
    }

    static List<MongoCredential> toList(List<SidistranMonCredential> list){
        List<MongoCredential> result = new ArrayList<MongoCredential>();
        for(SidistranMonCredential c : list){
            MongoCredential mongoCredential = c.buildCredential();
            if(c.buildCredential()!=null){
                result.add(mongoCredential);
            }
        }
        return result;
    }

    MongoCredential buildCredential(){
        if(this.userName!=null&&this.dataBase!=null&&this.password!=null) {
            if (this.mechanism == null) {
                return MongoCredential.createCredential(this.userName, this.dataBase, this.password);
            } else {
                if (this.mechanism == AuthenticationMechanism.GSSAPI)
                    return MongoCredential.createGSSAPICredential(userName);
                if (this.mechanism == AuthenticationMechanism.MONGODB_CR)
                    return MongoCredential.createMongoCRCredential(this.userName, this.dataBase, this.password);
                if (this.mechanism == AuthenticationMechanism.MONGODB_X509)
                    return MongoCredential.createMongoX509Credential(userName);
                if (this.mechanism == AuthenticationMechanism.PLAIN)
                    return MongoCredential.createPlainCredential(this.userName, this.dataBase, this.password);
                if (this.mechanism == AuthenticationMechanism.SCRAM_SHA_1)
                    return MongoCredential.createScramSha1Credential(this.userName, this.dataBase, this.password);
            }
        }
        return null;
    }

    public void setMechanism(String mechanism) {
        this.mechanism = AuthenticationMechanism.fromMechanismName(mechanism);
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public void setPassword(String password) {
        this.password = password.toCharArray();
    }

    public void setMechanismProperties(Map<String, Object> mechanismProperties) {
        this.mechanismProperties = mechanismProperties;
    }
}
