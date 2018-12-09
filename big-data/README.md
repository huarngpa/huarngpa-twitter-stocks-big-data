# Big Data Stack Overview
This is the big data package of the final project. For this component of the system I decided to break my project up into three parts:
1. Data ingest layer which ingests and builds the master dataset from Kafka queues.
2. Batch processing layer which builds batch views in Hive and transfers the views over to the serving layer (HBase).
3. Speed layer that interfaces and manages the HBase layer so the information is near real-time at the client/web layer.

# Big Data Architecture Considerations
I decided to skip the thrift serialization processes because getting batch data on a traditional file system is not aligned with the "big data" philosophy. Instead, I propose that a distributed, streaming ingest approach could be used to make our batch layer scalable.

I decided to break up the batch layer into "ingest" versus "batch processing" because I wanted the two layers to be able to operate at different speeds. Ingest should be relative fast since we need to clear the Kafka queue. While batch processing could be quite intensive and need to pipeline jobs in the right way so we can do complex batch processing jobs (like machine learning).

I decided to build by batch layer in Spark-Scala because Spark provides a lot of higher-level functionality and machine learning libraries to do useful analyses on the data at the batch layer.

For the speed layer...

# Technologies Used
* Batch Layer (Kafka, Spark, Scala):
    * Kafka (distributed, circular queue)
    * Hadoop, Hive, ORC (distributed storage)
    * Spark/Scala (map-reduce and parallelism)
    * Stanford NLP (sentiment analysis)
    * Spark MLLib (linear regression)
    * HBase (key-value store)
* Serving Layer (HBase)
* Speed Layer (Thrift, Python):
    * HBase Thrift (thrift server for HBase)
    * Tweepy (twitter API wrapper)
    * HappyBase 

# Batch Layer Installation & Deployment
If this is a first time deployment on your cluster, please run the hql contained in the `scripts` folder by running:
```
# For your local machine:
hive -f create_batch_layer_master_tables.hql

# For the cluster:
!connect jdbc:hive2://class-m-0-20181017030211.us-central1-a.c.mpcs53013-2018.internal:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
!run create_batch_layer_master_tables.hql
```

Then bring the batch layer up in the following sequence:
1. FinalSparkBatchIngest
2. speed-layer
3. FinalSparkBatchViews

Navigate to the `FinalSparkBatchIngest` folder, compile the project with `mvn install` and bring up the program by running:
```
# For your local machine:
spark-submit --master local[2] --class edu.uchicago.huarngpa.BatchIngest uber-FinalSparkBatchIngest-0.0.1-SNAPSHOT.jar localhost:9092

# For the cluster:

```

For the speed layer we need to make sure that the hbase thrift server has been started. So at the master node of the cluster run:
```
hbase thrift start
```

Then go to the web application node and run the program in the `speed-layer` folder:
```

```

Finally, we have all the necessary systems up, we can run the batch processing subsytem by going back the master node of the big data cluster and using the project in the `FinalSparkBatchViews` folder. Compile the project with `mvn install` and bring up the program by running:
```
# For your local machine:
spark-submit --master local[2] --class edu.uchicago.huarngpa.BatchViews uber-FinalSparkBatchViews-0.0.1-SNAPSHOT.jar localhost:2181

# For the cluster:

```
