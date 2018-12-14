# Big Data Stack Overview
This is the big data package of the final project. For this component of the system I decided to break my project up into three parts:
+ Data ingest layer which ingests and builds the master dataset from Kafka queues.
+ Batch processing layer which builds batch views in Hive and transfers the views over to the serving layer (HBase).
+ Speed layer that interfaces and manages the HBase layer so the information is near real-time at the client/web layer.

# Big Data Architecture Considerations
I decided to skip the thrift serialization processes because getting batch data on a traditional file system is not aligned with the "big data" philosophy (ie. using bash scripts to save data first on the local drive). Instead, I propose that a distributed, streaming ingest approach could be used to make our batch layer scalable.

I decided to break up the batch layer into "ingest" versus "batch processing" because I wanted the two layers to be able to operate at different speeds. Ingest should be relative fast since we need to clear the Kafka queue at a reasonably fast rate. While batch processing could be quite compute-intensive and needs to pipeline jobs in the right order for complex processing (like machine learning).

I decided to build by batch layer in Spark-Scala because Spark provides a lot of higher-level functionality and machine learning libraries to do useful analyses on the data at the batch layer. Scala was my language of choice, because I could experiment with the language in the console and could write my programs in a script-like and functional style.

For the speed layer I decided to switch back to python because I needed to prototype the application quickly. The speed layer is relatively "dumb", all this application does is polls the data sources every hour to get more data, process it into a separate key-value store at the serving layer and sends writes the new data to the kafka queue so the batch layer can eventually process it.

Because we might have duplication within our batch layer, I designed the batch layer to be impervious to duplicate records, that is we have a normalization batch view that deduplicates the data and then the resulting batch views can use the "normalized" view to process data correctly.

Probably the most useful features of big data is the ability to combine different datasets to identify trends between the two datasets. For this project I used the Stanford NLP library and implemented a sentiment analysis on the ingested twitter data. Since this is a batch view, we can change our sentiment analysis model later on and recompute the results.

Then using the Spark MLLib, I run a linear regressor on the data and store the computed results in Hive.

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

Note that for batch ingestion there are two projects that need to be compiled and run with `mvn install`. See the `FinalSparkBatchIngestStock` and `FinalSparkBatchIngestTwitter` projects:
```
# For your local machine:
spark-submit --master local[2] --class edu.uchicago.huarngpa.BatchIngest uber-FinalSparkBatchIngestStock-0.0.1-SNAPSHOT.jar localhost:9092
spark-submit --master local[2] --class edu.uchicago.huarngpa.BatchIngest uber-FinalSparkBatchIngestTwitter-0.0.1-SNAPSHOT.jar localhost:9092

# For the cluster:
spark-submit --master yarn --deploy-mode client --class edu.uchicago.huarngpa.BatchIngest uber-FinalSparkBatchIngestStock-0.0.1-SNAPSHOT.jar 10.0.0.2:6667
spark-submit --master yarn --deploy-mode client --class edu.uchicago.huarngpa.BatchIngest uber-FinalSparkBatchIngestTwitter-0.0.1-SNAPSHOT.jar 10.0.0.2:6667
```

For the speed layer we need to make sure that the hbase thrift server has been started. So at the master node of the cluster run:
```
hbase thrift start
```

On the web server, deploy the application in the `speed-layer` folder by making sure the following variables have been sourced to the environment:
```
# For your local machine:
export TWITTER_API_KEY="<your key>"
export TWITTER_API_SECRET_KEY="<your secret-key>"
export TWITTER_API_ACCESS_TOKEN="<your token>"
export TWITTER_API_ACCESS_TOKEN_SECRET="<your token-secret>"
export API_KAFKA_HOST="0.0.0.0"
export API_KAFKA_PORT="9092"
export API_HBASE_THRIFT_HOST="0.0.0.0"
export API_HBASE_THRIFT_PORT="9090"

# For the cluster:
export TWITTER_API_KEY="<your key>"
export TWITTER_API_SECRET_KEY="<your secret-key>"
export TWITTER_API_ACCESS_TOKEN="<your token>"
export TWITTER_API_ACCESS_TOKEN_SECRET="<your token-secret>"
export API_KAFKA_HOST="10.0.0.2"
export API_KAFKA_PORT="6667"
export API_HBASE_THRIFT_HOST="10.0.0.2"
export API_HBASE_THRIFT_PORT="9090"
```

Then go to the web application node and run the program in the `speed-layer` folder:
```
./run-speed-layer.sh
```

Finally, we have all the necessary systems up, we can run the batch processing subsytem by going back the master node of the big data cluster and using the project in the `FinalSparkBatchViews` folder. Compile the project with `mvn install` and bring up the program by running:
```
# For your local machine:
spark-submit --master local[2] --class edu.uchicago.huarngpa.BatchViews uber-FinalSparkBatchViews-0.0.1-SNAPSHOT.jar 0.0.0.0

# For the cluster:
spark-submit --master yarn --deploy-mode client --class edu.uchicago.huarngpa.BatchViews uber-FinalSparkBatchViews-0.0.1-SNAPSHOT.jar 10.0.0.2
```
