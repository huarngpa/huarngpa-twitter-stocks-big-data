package edu.uchicago.huarngpa
import edu.uchicago.huarngpa.IngestKafkaTwitter

/**
 * Coordinates the master dataset creation in Hive, batch view
 * creation and writes to HBase.
 */
object SparkBatchLayer {


  def main(args:Array[String]) {

    println(s"""
      |Batch system running!! Logging to stdout. 
      |Will run batch programs every eight (8) hours.""");

    val twitter = IngestKafkaTwitter;
    twitter.run()
  }
}
