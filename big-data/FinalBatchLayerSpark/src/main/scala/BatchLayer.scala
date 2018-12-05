package edu.uchicago.huarngpa
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.StreamingContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uchicago.huarngpa.{StockRecord, TwitterRecord}


/**
 * Batch layer ingest and view creation.
 */
object BatchLayer {
  
  val mapper = new ObjectMapper();
  mapper.registerModule(DefaultScalaModule);
  
  val sparkConf = new SparkConf().setAppName("SparkBatchLayer");
  val streamingContext = new StreamingContext(sparkConf, Seconds(30));
  streamingContext.checkpoint("/tmp/huarngpa/batch/twitter")
  val hiveContext = new HiveContext(streamingContext.sparkContext);

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  );

  /**
   * Creates the batch layer data by reading from a Kafka queue
   * writing data to a temporary table and then appending to a 
   * Hive ORC table.
   */
  def batchLayerIngestKafkaTwitter(): Unit = {

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext, 
      PreferConsistent,
      Subscribe[String, String](
        Array("huarngpa_ingest_batch_twitter"), 
        kafkaParams)
    );

    val records = stream.map(_.value);

    val serializedRecords = records.map(
      record => mapper.readValue(record, classOf[TwitterRecord])
    );

    val serializedRows = serializedRecords.map(
      x => Row(
        x.created_at,                            // col0
        x.created_at_day,                        // col1
        x.id_str,                                // col2
        x.text,                                  // col3
        x.retweeted_count,                       // col4
        x.favorite_count,                        // col5
        x.user_id                                // col6
      )
    );

    val schema = List(
      StructField("col0", StringType, true),
      StructField("col1", StringType, true),
      StructField("col2", StringType, true),
      StructField("col3", StringType, true),
      StructField("col4", LongType, true),
      StructField("col5", LongType, true),
      StructField("col6", StringType, true)
    );

    serializedRows.foreachRDD(rdd => {
      val df = hiveContext.createDataFrame(
        rdd,
        StructType(schema)
      ).withColumn( // patches the str field to date field
        "col1",
        to_date(col("col1"), "yyyy-MM-dd")
      );
      df.registerTempTable("huarngpa_tmp_master_twitter")
      df.write
        .mode(SaveMode.Append)
        .format("hive")
        .saveAsTable("huarngpa_master_twitter");
    });
    
  }
  
  /**
   * This is the stock version of the ingest master data routine.
   */
  def batchLayerIngestKafkaStock(): Unit = {

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext, 
      PreferConsistent,
      Subscribe[String, String](
        Array("huarngpa_ingest_batch_stock"), 
        kafkaParams)
    );

    val records = stream.map(_.value);

    val serializedRecords = records.map(
      record => mapper.readValue(record, classOf[StockRecord])
    );

    val serializedRows = serializedRecords.map(
      x => Row(
        x.ticker,         // col0
        x.date,           // col1
        x.day_open,       // col2
        x.day_high,       // col3
        x.day_low,        // col4
        x.day_close,      // col5
        x.day_volume      // col6
      )
    );

    //case class StockRecord(
    //  ticker: String,
    //  date: String, 
    //  day_open: Double, 
    //  day_high: Double, 
    //  day_low: Double, 
    //  day_close: Double, 
    //  day_volume: Long
    //)

    val schema = List(
      StructField("col0", StringType, true),
      StructField("col1", StringType, true),
      StructField("col2", DoubleType, true),
      StructField("col3", DoubleType, true),
      StructField("col4", DoubleType, true),
      StructField("col5", DoubleType, true),
      StructField("col6", LongType, true)
    );

    serializedRows.foreachRDD(rdd => {
      val df = hiveContext.createDataFrame(
        rdd,
        StructType(schema)
      ).withColumn( // patches the str field to date field
        "col1",
        to_date(col("col1"), "yyyy-MM-dd")
      );
      df.registerTempTable("huarngpa_tmp_master_stock")
      df.write
        .mode(SaveMode.Append)
        .format("hive")
        .saveAsTable("huarngpa_master_stock");
    });
    
  }
  
  def main(args:Array[String]) {
    batchLayerIngestKafkaTwitter();
    batchLayerIngestKafkaStock();
    streamingContext.start();
    streamingContext.awaitTermination();

  }
}
