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
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.DefaultScalaModule


/**
 * Batch layer ingest and view creation.
 */
object BatchIngest {
  

  /**
   * Creates the batch layer data by reading from a Kafka queue
   * writing data to a temporary table and then appending to a 
   * Hive ORC table.
   *
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

    val Array(brokers) = args
    
    val spark = SparkSession.builder()
                            .appName("SparkBatchLayerStock")
                            .enableHiveSupport()
                            .getOrCreate()

    val streamingContext = new StreamingContext(spark.sparkContext, 
                                                Seconds(30));
    streamingContext.checkpoint("/tmp/huarngpa/batch/stock");

    val topicsSet = Set("huarngpa_ingest_batch_stock")
    
    val sqlContext = new SQLContext(streamingContext.sparkContext);
    import sqlContext.implicits._

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    
    val stream = KafkaUtils.createDirectStream[String, 
                                               String, 
                                               StringDecoder,
                                               StringDecoder](
      streamingContext,
      kafkaParams,
      topicsSet
    );

    val records = stream.map(_._2);

    val serializedRecords = records.map(record => {
      val mapper = new ObjectMapper();
      mapper.registerModule(DefaultScalaModule);
      mapper.readValue(record, classOf[StockRecord])
    });

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

    // stream and run
    streamingContext.start();
    streamingContext.awaitTermination();

  }
}
