package edu.uchicago.huarngpa
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveContext
import edu.uchicago.huarngpa.Sentiment.Sentiment


/**
 * Batch layer view creation program.
 */
object BatchViews {
  
  val hbaseConf: Configuration = HBaseConfiguration.create()
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
   * for debugging the application in Hive
   */
  def batchViewsTwitterAllTime(): Unit = {
    print("Starting twitter all time batch view... ")
    val twitterAllTime = spark.sql(s"""
      |select 
      |  user_id as user_id,
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
   * Creates basic descriptive statistics around the twitter data
   * to be used later by the application layer to coordinate the 
   * batch and speed layers.
   *
   * Note that today's data is not compiled as part of the batch
   * view. To be coordinated with the speed layer.
   */
  def batchViewsTwitterWeekly(): Unit = {
    print("Starting twitter weekly batch view... ")
    val twitterWeekly = spark.sql(s"""
      |select 
      |  user_id as user_id,
      |  count(tweet_id) as count_tweets,
      |  sum(retweets) as sum_retweets,
      |  sum(favorited) as sum_favorited
      |from 
      |  huarngpa_view_twitter_normalized
      |where
      |  tweet_date between 
      |    cast(to_date(from_unixtime(unix_timestamp()-86400*7)) as date) and 
      |    cast(to_date(from_unixtime(unix_timestamp()-86400)) as date)
      |group by 
      |  user_id
      """.stripMargin
    );

    twitterWeekly
      .registerTempTable("huarngpa_tmp_view_twitter_weekly");
    twitterWeekly.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_twitter_weekly");

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
      drop table if exists huarngpa_view_twitter_sentiment
      """.stripMargin)
    spark.sql(s"""
      |create 
      |  table huarngpa_view_twitter_sentiment 
      |    as 
      |      select 
      |        tweet_id as tweet_id,
      |        avg(sentiment) as sentiment
      |      from 
      |        huarngpa_tmp_view_twitter_sentiment
      |      group by
      |        tweet_id
      """.stripMargin
      );

    print("Completed.\n");
  }
  
  /*
   * Compute the 
   */
  def batchViewsTwitterSentiment(): Unit = {
  }
  
  /*
   * Since we may have many stock events getting ingested by
   * the data lake, we need to normalize the data prior to
   * consuming it
   */
  def batchViewsStockNormalized(): Unit = {
    print("Starting stock normalization batch view... ")
    val stockNormalized = spark.sql(s"""
      |select 
      |  B.ticker as ticker,
      |  B.trading_day as trading_day,
      |  B.received as received,
      |  A.col2 as day_open,
      |  A.col3 as day_high,
      |  A.col4 as day_low,
      |  A.col5 as day_close,
      |  (A.col5-A.col2)/A.col2 as day_change,
      |  A.col6 as day_volume
      |from 
      |  huarngpa_master_stock A
      |inner join
      |  (select
      |    col0 as ticker,
      |    col1 as trading_day,
      |    max(col7) as received
      |  from 
      |    huarngpa_master_stock
      |  group by
      |    col0,
      |    col1) B
      |on
      |  B.ticker = A.col0 and
      |  B.trading_day = A.col1 and
      |  B.received = A.col7
      """.stripMargin
    );
    stockNormalized
      .registerTempTable("huarngpa_tmp_view_stock_normalized");
    stockNormalized.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_stock_normalized");
    print("Completed.\n");
  }
  
  /*
   * Creates basic descriptive statistics around the stock data
   * for debugging the application in Hive
   */
  def batchViewsStockAllTime(): Unit = {
    print("Starting stock all time batch view... ")
    val stockAllTime = spark.sql(s"""
      |select 
      |  ticker as ticker,
      |  count(trading_day) as count_trading_days,
      |  sum(day_open) as sum_day_open,
      |  max(day_high) as max_day_high,
      |  min(day_low) as min_day_low,
      |  sum(day_close) as sum_day_close,
      |  sum(day_change) as sum_day_change,
      |  sum(day_volume) as sum_day_volume
      |from huarngpa_view_stock_normalized
      |group by ticker
      """.stripMargin
    );

    stockAllTime
      .registerTempTable("huarngpa_tmp_view_stock_alltime");
    stockAllTime.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_stock_alltime");

    print("Completed.\n");
  }
  
  /*
   * Creates basic descriptive statistics around the stock data
   * to be used later by the application layer to coordinate the 
   * batch and speed layers.
   *
   * Note that today's data is not compiled as part of the batch
   * view. To be coordinated with the speed layer.
   */
  def batchViewsStockWeekly(): Unit = {
    print("Starting stock weekly batch view... ")
    val stockWeekly = spark.sql(s"""
      |select 
      |  ticker as ticker,
      |  count(trading_day) as count_trading_days,
      |  sum(day_open) as sum_day_open,
      |  max(day_high) as max_day_high,
      |  min(day_low) as min_day_low,
      |  sum(day_close) as sum_day_close,
      |  sum(day_change) as sum_day_change,
      |  sum(day_volume) as sum_day_volume
      |from huarngpa_view_stock_normalized
      |where
      |  trading_day between 
      |    cast(to_date(from_unixtime(unix_timestamp()-86400*7)) as date) and 
      |    cast(to_date(from_unixtime(unix_timestamp()-86400)) as date)
      |group by 
      |  ticker
      """.stripMargin
    );

    stockWeekly
      .registerTempTable("huarngpa_tmp_view_stock_weekly");
    stockWeekly.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_stock_weekly");

    print("Completed.\n");
  }
  
  /*
   * Describe
   */
  def batchViewsSentimentStockLinReg(): Unit = {

    val sentiment = spark.table("huarngpa_view_twitter_sentiment");
    val twitter = spark.table("huarngpa_view_twitter_normalized");
    val stock = spark.table("huarngpa_view_stock_normalized");

    val ts = twitter.join(
      sentiment, twitter("tweet_id") <=> sentiment("tweet_id")
    );

    val ts2 = ts.groupBy(
      ts("tweet_date").as("date"),
      ts("user_id")
    ).agg(
      avg("sentiment").as("avg_sentiment"), 
      sum("retweets").as("sum_retweets"), 
      sum("favorited").as("sum_favorited")
    );

    val cols = Seq(
      "ticker", 
      "date", 
      "received", 
      "day_open", 
      "day_high", 
      "day_low", 
      "day_close", 
      "day_volume"
    );

    val stock2 = stock.toDF(cols: _*)

  }
  
  def main(args: Array[String]) = {
    
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: spark-submit ... "<host1,host2,host3>"
        |  <host1,...,hostn> for zookeeper quorum.
        """.stripMargin);
      System.exit(1);
    }
  
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", args(0))
    
    while (true) {
      // run the batch layer pipeline
      batchViewsTwitterNormalize()
      batchViewsTwitterAllTime()
      batchViewsTwitterWeekly()
      batchViewsStockNormalized()
      batchViewsStockAllTime()
      batchViewsStockWeekly()
      // data science job, compute intensive
      batchViewsTwitterSentiment()
      batchViewsWeeklyTwitterSentiment()
      //batchViewsSentimentStockLinReg()
      Thread.sleep(eightHours)
    }

  }
}
