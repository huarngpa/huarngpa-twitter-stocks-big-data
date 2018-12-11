package edu.uchicago.huarngpa
import org.apache.kafka.clients.consumer.ConsumerRecord
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
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

  var kafkaParams = Map[String, String]();

  /**
   * Bootstraps the kafka params for the batch layer.
   */
  def bootstrapKafkaParams(server: String): Unit = {
    kafkaParams = Map[String, String]("metadata.broker.list" -> server)
  }

  /**
   * Creates the batch layer data by reading from a Kafka queue
   * writing data to a temporary table and then appending to a 
   * Hive ORC table.
   */
  def batchLayerIngestKafkaTwitter(): Unit = {

    val stream = KafkaUtils.createDirectStream[String, 
                                               String, 
                                               StringDecoder,
                                               StringDecoder](
      streamingContext,
      kafkaParams,
      Set("huarngpa_ingest_batch_twitter")
    );

    val records = stream.map(_._2);

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
      df.show()
    });
  }
  
  /**
   * This is the stock version of the ingest master data routine.
   */
  def batchLayerIngestKafkaStock(): Unit = {

    val stream = KafkaUtils.createDirectStream[String, 
                                               String, 
                                               StringDecoder,
                                               StringDecoder](
      streamingContext,
      kafkaParams,
      Set("huarngpa_ingest_batch_stock")
    );

    val records = stream.map(_._2);

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
        x.day_volume,     // col6
        x.received        // col7
      )
    );

    val schema = List(
      StructField("col0", StringType, true),
      StructField("col1", StringType, true),
      StructField("col2", DoubleType, true),
      StructField("col3", DoubleType, true),
      StructField("col4", DoubleType, true),
      StructField("col5", DoubleType, true),
      StructField("col6", LongType, true),
      StructField("col7", DoubleType, true)
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
      df.show()
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
