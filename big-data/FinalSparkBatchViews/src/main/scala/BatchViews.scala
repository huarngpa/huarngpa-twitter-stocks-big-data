package edu.uchicago.huarngpa
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveContext
import edu.uchicago.huarngpa.SentimentAnalyzer
import edu.uchicago.huarngpa.Sentiment.Sentiment


/**
 * Batch layer view creation program.
 */
object BatchViews {
  
  val spark = SparkSession.builder()
                          .appName("SparkBatchViews")
                          .enableHiveSupport()
                          .getOrCreate();
  val eightHours = 1000 * 60 * 60 * 8;
  
  /*
   * Since we might receive duplicate entries for the same tweet
   * (from batch or speed layer) we need to account for these errors
   * and normalize the data so that we're creating downstream batch
   * views with a normalized data set.
   */
  def batchViewsTwitterNormalize(): Unit = {
    print("Starting twitter normalization batch view... ")
    val twitterNormalized = spark.sql(s"""
      |select 
      |  col1 as tweet_date,
      |  col2 as tweet_id,
      |  max(col4) as retweets,
      |  max(col5) as favorited,
      |  col6 as user_id
      |from 
      |  huarngpa_master_twitter
      |group by 
      |  col1,
      |  col2,
      |  col6
      """.stripMargin
    );
    twitterNormalized
      .registerTempTable("huarngpa_tmp_view_twitter_normalized");
    twitterNormalized.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_twitter_normalized");
    print("Completed.\n");
  }

  /*
   * Creates basic descriptive statistics around the twitter data
   * to be used later by the applicaiton layer to coordinate the 
   * batch and speed layers.
   */
  def batchViewsTwitterAllTime(): Unit = {
    print("Starting twitter all time batch view... ")
    val twitterAllTime = spark.sql(s"""
      |select 
      |  user_id as user_id,
      |  min(tweet_date) as min_date,
      |  max(tweet_date) as max_date,
      |  count(tweet_id) as count_tweets,
      |  sum(retweets) as sum_retweets,
      |  sum(favorited) as sum_favorited
      |from huarngpa_view_twitter_normalized
      |group by user_id
      """.stripMargin
    );

    twitterAllTime
      .registerTempTable("huarngpa_tmp_view_twitter_alltime");
    twitterAllTime.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_twitter_alltime");

    print("Completed.\n");
  }
  
  /*
   * Runs Stanford NLP Sentiment analyses on tweets.
   */
  def batchViewsTwitterSentiment(): Unit = {

    print("Starting twitter sentiment analysis... ")
    
    val df = spark.table("huarngpa_master_twitter");

    def matchEnum(x: Sentiment): Double = x match {
      case Sentiment.VNEGATIVE => -1.0
      case Sentiment.NEGATIVE => -0.5
      case Sentiment.POSITIVE => 0.5
      case Sentiment.VPOSITIVE => 1.0
      case _ => 0.0
    }
    
    import spark.sqlContext.implicits._
    val df2 = df.map(r => {
      val tweetId = r.getAs[String]("col2");
      val tweetText = r.getAs[String]("col3")
                       .replaceAll("\n", "")
                       .replaceAll("rt\\s+", "")
                       .replaceAll("\\s+@\\w+", "")
                       .replaceAll("@\\w+", "")
                       .replaceAll("\\s+#\\w+", "")
                       .replaceAll("#\\w+", "")
                       .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
                       .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
                       .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
                       .replaceAll("(?:https?|http?)//[\\w/%.-]+", "");
      try {
        val sentiment = matchEnum(SentimentAnalyzer.mainSentiment(tweetText));
        (tweetId, sentiment)
      } catch {
        case _: Throwable => (tweetId, 0.0)
      }
    });

    val cols = Seq("tweet_id", "sentiment");
    val df3 = df2.toDF(cols: _*)

    df3.createOrReplaceTempView("huarngpa_tmp_view_twitter_sentiment");
    spark.sql(s"""
      |create 
      |  table huarngpa_view_twitter_sentiment 
      |    as select * from huarngpa_tmp_view_twitter_sentiment
      """.stripMargin
      );

    print("Completed.\n");
  }
  
  /*
   * Describe
   */
  def batchViewsStockNormalized(): Unit = {}
  
  /*
   * Describe
   */
  def batchViewsStockWeekly(): Unit = {}
  
  def main(args: Array[String]) = {
    
    while (true) {
      // run the batch layer pipeline
      batchViewsTwitterNormalize()
      batchViewsTwitterAllTime()
      batchViewsTwitterSentiment()
      Thread.sleep(eightHours)
    }

  }
}
