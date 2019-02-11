# SidistranMongoDriver
A MongoDB java driver, with MVCC. So, you can use it to perform Transaction Transactional with your code

PS: in the newest version, we can perform a "distributed transaction"

1) not only cross over mongodb but could cross over mongodb and mysql
2) not only one mongodb-server or mysql-server but any differents
3) support spring annoation

contact me (mail:11509923@qq.com) if you have interesting.


### **1. if your working db already has datas, you must do this:**
`db.XX.update({}, {$set:{__s_.__s_stat: 2, __s_.__s_g_time:Long.MAX_VALUE,__s_.__s_c_txid:-1l,__s_.__s_u_txid:-1l})`

#### **very important**
```
  1）your connection user must can see the DB named “transaction”
  2）the user with credential must can [readWrite] DB-“transaction”
  4）if your collection already has an uniqueindex.you must:
     fisrt drop it,
     then recreate it but add a field named "unique_" to the index.
     i.e.: consider Collection A has  uniqueindex that name="test", field="aaaa".
           this index should be update to name="test", field="aaaa, unique_".
     !!!!OR!!!!YOU CAN CREATE INDEX VIA THIS DRIVER!!!IT WILL AUTOMATIC CREATE THE "unique_" FIELD" 
     
     
  CAUSTIONS:
  1）do not support DBRef with _id，because on update will create new temp-obj with new _id;
```
#Or you can do these operations in your own codes ^_^#


### **2.config SidistranMongoClient int Spring**
`can not use “mongo” label，you should config a <bean id="mongo" class="SidistranMongoClient"> instead`


### **3.demo:**
```java
   SidistranMongoClient mongo = new SidistranMongoClient(serverAddress, credentials);

   MongoTanscationManager tanscationManager = new MongoTanscationManager(mongo);//this is singleton

   tanscationManager.begin();//begin a transaction

   do_with(mongo);

   tanscationManager.commit();//commit it

   tanscationManager.close();//you must close the MongoTanscationManager if won't use anymore
   mongo.close();
```

