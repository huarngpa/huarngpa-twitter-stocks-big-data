package edu.uchicago.huarngpa
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveContext


/**
 * Batch layer view creation program.
 */
object BatchViews {
  
  val spark = SparkSession.builder()
                          .appName("SparkBatchViews")
                          .enableHiveSupport()
                          .getOrCreate();
  val eightHours = 1000 * 60 * 60 * 8;
  
  def batchViewsTwitterNormalize(): Unit = {
    print("Starting twitter normalization batch view... ")
    spark.sql(s"""
      |select 
      |  A.col1 as tweet_date,
      |  A.col2 as tweet_id,
      |  B.col3 as tweet_text,
      |  max(A.col4) as retweets,
      |  max(A.col5) as favorited,
      |  A.col6 as user_id
      |from 
      |  huarngpa_master_twitter A
      |left join
      |  huarngpa_master_twitter B on
      |    A.col1 = B.col1 and 
      |    A.col2 = B.col2 and
      |    A.col6 = B.col6
      |group by 
      |  A.col1,
      |  A.col2,
      |  B.col3,
      |  A.col6
      """.stripMargin
    ).write.mode(SaveMode.Overwrite)
     .saveAsTable("huarngpa_view_twitter_normalized");
    print("Completed.\n");
  }

  def batchViewsTwitterAllTime(): Unit = {
    print("Starting twitter all time batch view... ")
    spark.sql(s"""
      |select 
      |  user_id as user_id,
      |  min(tweet_date) as min_date,
      |  max(tweet_date) as max_date,
      |  count(tweet_id) as count_tweets,
      |  sum(retweets) as sum_retweets,
      |  sum(favorited) as sum_favorited
      |from huarngpa_view_twitter_normalized
      |group by col6
      """.stripMargin
    ).show();
    //).write.mode(SaveMode.Overwrite)
    // .saveAsTable("huarngpa_view_twitter_all_time");
    print("Completed.\n");
  }
  
  def batchViewsStockNormalize(): Unit = {
    print("Starting twitter normalization batch view... ")
    spark.sql(s"""
      |select 
      |  A.col1 as tweet_date,
      |  A.col2 as tweet_id,
      |  B.col3 as tweet_text,
      |  max(A.col4) as retweets,
      |  max(A.col5) as favorited,
      |  A.col6 as user_id
      |from 
      |  huarngpa_master_twitter A
      |left join
      |  huarngpa_master_twitter B on
      |    A.col1 = B.col1 and 
      |    A.col2 = B.col2 and
      |    A.col6 = B.col6
      |group by 
      |  A.col1,
      |  A.col2,
      |  B.col3,
      |  A.col6
      """.stripMargin
    ).write.mode(SaveMode.Overwrite)
     .saveAsTable("huarngpa_view_twitter_normalized");
    print("Completed.\n");
  }
  
  def main(args: Array[String]) = {
    
    while (true) {
      //spark.sql("show tables").show()
      spark.sql("select * from huarngpa_master_twitter").show()
      //batchViewsTwitterNormalize()
      //batchViewsTwitterAllTime()
      Thread.sleep(eightHours)
    }

  }
}
