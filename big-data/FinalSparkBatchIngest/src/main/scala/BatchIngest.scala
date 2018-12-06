package edu.uchicago.huarngpa
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
object BatchIngest {
  
  val mapper = new ObjectMapper();
  mapper.registerModule(DefaultScalaModule);
  
  val spark = SparkSession.builder()
                          .appName("SparkBatchLayer")
                          .enableHiveSupport()
                          .getOrCreate()
  val streamingContext = new StreamingContext(spark.sparkContext, 
                                              Seconds(30));
  streamingContext.checkpoint("/tmp/huarngpa/batch/");
  val sqlContext = new SQLContext(streamingContext.sparkContext);
  import sqlContext.implicits._

  var kafkaParams = Map[String, Object]();

  /**
   * Bootstraps the kafka params for the batch layer.
   */
  def bootstrapKafkaParams(server: String): Unit = {
    kafkaParams = Map[String, Object](
      "bootstrap.servers" -> server,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );
  }

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
      val df = sqlContext.createDataFrame(
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
      val df = sqlContext.createDataFrame(
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

  /*
   * Main entry point for the batch layer program. 
   * Takes a <host:port> param for the kafka machine and initializes
   * a streaming session to ingest batch layer data from the queues.
   */
  def main(args: Array[String]) = {

    if (args.length < 1) {
      System.err.println(s"""
        |Usage: spark-submit ... <host:port>
        |  <host:port> for the leader machine to coordinate the batch jobs.
        """.stripMargin);
      System.exit(1);
    }

    bootstrapKafkaParams(args(0));

    // master and batch views 
    batchLayerIngestKafkaTwitter();
    batchLayerIngestKafkaStock();

    // stream and run
    streamingContext.start();
    streamingContext.awaitTermination();

  }
}
