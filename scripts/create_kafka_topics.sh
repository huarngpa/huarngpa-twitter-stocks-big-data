#!/bin/bash

##########################################################################
# CREATE KAFKA TOPICS
##########################################################################
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic huarngpa_ingest_batch_stock
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic huarngpa_ingest_batch_twitter
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic huarngpa_ingest_speed_stock
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic huarngpa_ingest_speed_twitter
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper localhost:2181 | grep huarngpa
