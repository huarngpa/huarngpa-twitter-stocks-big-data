# Big Data Stack Overview
This is the big data package of the final project. For this part of the stack, I relied on the core technologies we learned about in class (Hadoop, Hive, HBase, Spark), but chose to focus on Spark as the primary point of interface/coordination of the batch, serving, and speed layers.

## Data Lake Layer

## Batch Layer
## Serving Layer
## Speed Layer

# Big Data Architecture Considerations
At the batch layer we ingest data by reading it off of a Kafka topic that our backend web application wrote to. The batch layer is written in Scala and takes advantage of Spark features and functionality to handle ingestion, batch processing, and machine learning jobs that need to be handled at the batch layer.

# Technologies Used
* Data Lake and Batch layer
    * Hadoop
    * Hive
    * ORC
    * Spark
    * Stanford CoreNLP
* Serving layer
* Speed layer

# Data Lake Layer Installation & Deployment
If this is a first time deployment on your cluster, please run the hql contained in the `scripts/create_batch_layer_master_tables.hql`. This builds the tables we for the master dataset.

To start the batch layer please run:
```
# For local machine:
spark-submit --master local[2] --class edu.uchicago.huarngpa.BatchIngest uber-FinalSparkBatchIngest-0.0.1-SNAPSHOT.jar localhost:9092

# For cluster:
```

# Backend Layer Installation & Deployment
```
# For local machine:
spark-submit --master local[2] --css edu.uchicago.huarngpa.BatchViews uber-FinalSparkBatchViews-0.0.1-SNAPSHOT.jar localhost:9092

# For cluster:
```

# Serving Layer Installation & Deployment
# Speed Layer Installation & Deployment
