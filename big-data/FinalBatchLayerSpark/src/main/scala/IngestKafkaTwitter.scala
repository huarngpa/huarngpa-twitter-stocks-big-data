package edu.uchicago.huarngpa
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.StreamingContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uchicago.huarngpa.TwitterRecord


/**
 * Gets Twitter data from a Kafka topic and writes the data
 * into Hive.
 */
object IngestKafkaTwitter {
  
  val mapper = new ObjectMapper();
  mapper.registerModule(DefaultScalaModule);
  
  val sparkConf = new SparkConf().setAppName("SparkBatchLayer");
  val streamingContext = new StreamingContext(sparkConf, Seconds(30));
  val hiveContext = new HiveContext(streamingContext.sparkContext);

  val topics = Array("huarngpa_ingest_batch_twitter")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  );

  def run(): Unit = {

    println("Starting twitter data ingestion from Kafka to Hive.");

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext, 
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    );

    val records = stream.map(_.value);
    val serializedRecords = records.map(
      record => mapper.readValue(record, classOf[TwitterRecord])
    );
    val serializedSplitted = serializedRecords.map(
      x => Row(
        x.created_at,
        x.created_at_day,
        x.id_str,
        x.text,
        x.retweeted_count,
        x.favorite_count,
        x.user_id
      )
    );

    serializedSplitted.print();
    //val df = hiveContext.createDataFrame(streamingContext, )
    
    streamingContext.start();
    streamingContext.awaitTermination();
  }
}
