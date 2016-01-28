# SidistranMongoDriver
A MongoDB java driver, with MVCC. So, you can use it to deal Transaction with your code


### **1. if your working db already has datas, you must do this:**
`db.XX.update({}, {$set:{__s_.__s_stat:2}})`

```
 very important
  1）your connection must can see the DB named “transaction”
  2）the user with credential must can readWrite DB-“transaction”
```

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

