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

    df3
      .registerTempTable("huarngpa_tmp_view_twitter_sentiment");
    df3.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_twitter_sentiment");

    print("Completed.\n");
  }
  
  /*
   * Compute the weekly sentiment on twitter
   */
  def batchViewsWeeklyTwitterSentiment(): Unit = {
    print("Starting weekly twitter sentiment batch view... ")
    val weeklyTwitterSentiment = spark.sql(s"""
      |select
      |  C.user_id as user_id,
      |  sum(C.sentiment) as sum_sentiment
      |from
      |  (select
      |     B.user_id,
      |     A.tweet_id as tweet_id,
      |     A.sentiment as sentiment,
      |     B.tweet_date as tweet_date
      |   from 
      |     (select
      |        tweet_id as tweet_id,
      |        avg(sentiment) as sentiment
      |      from huarngpa_view_twitter_sentiment
      |      group by tweet_id) A
      |   inner join
      |     huarngpa_view_twitter_normalized B
      |       on A.tweet_id = B.tweet_id
      |   ) C
      |where
      |  C.tweet_date between 
      |    cast(to_date(from_unixtime(unix_timestamp()-86400*7)) as date) and 
      |    cast(to_date(from_unixtime(unix_timestamp()-86400)) as date)
      |group by
      |  C.user_id
      """.stripMargin
    );
    weeklyTwitterSentiment
      .registerTempTable("huarngpa_tmp_view_twitter_weekly_sentiment");
    weeklyTwitterSentiment.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("huarngpa_view_twitter_weekly_sentiment");
    print("Completed.\n");
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
   * Prep the data for linear regression analysis
   */
  def batchJoinTwitterAndStock(): Unit = {
    
    print("Starting stock weekly batch view... ")

    val twitterUsers = spark.sql(s"""
      |select user_id 
      |from huarngpa_view_twitter_normalized
      |group by user_id
      """.stripMargin
    )

    twitterUsers.foreach(u => {

      val stockTickers = spark.sql(s"""
        |select ticker
        |from huarngpa_view_stock_normalized
        |group by ticker
        """.stripMargin
      )

      stockTickers.foreach(t => {

        val user = u.getString(0)
        val ticker = t.getString(0)

        val twitter = spark.sql(s"""
          |select
          |  C.user_id as user_id,
          |  C.tweet_date as tweet_date,
          |  avg(C.sentiment) as avg_sentiment
          |from
          |  (select
          |     B.user_id,
          |     A.tweet_id as tweet_id,
          |     A.sentiment as sentiment,
          |     B.tweet_date as tweet_date
          |   from 
          |     (select
          |        tweet_id as tweet_id,
          |        avg(sentiment) as sentiment
          |      from huarngpa_view_twitter_sentiment
          |      group by tweet_id) A
          |   inner join
          |     huarngpa_view_twitter_normalized B
          |       on A.tweet_id = B.tweet_id
          |   ) C
          |group by
          |  C.user_id,
          |  C.tweet_date
          """.stripMargin
        );
        val stocks = spark.table("huarngpa_view_stock_normalized");

        val filteredTwitter = twitter.filter(twitter("user_id").equalTo(user))
        val filteredStock = stocks.filter(stocks("ticker").equalTo(ticker))

        filteredTwitter.registerTempTable("huarngpa_tmp_filtered_twitter")
        filteredStock.registerTempTable("huarngpa_tmp_filtered_stock")

        val df = spark.sql(s"""
          |select
          |  A.user_id as user_id,
          |  B.ticker as ticker,
          |  A.tweet_date as join_date,
          |  A.avg_sentiment as sentiment,
          |  B.day_change as day_change
          |from 
          |  huarngpa_tmp_filtered_twitter A
          |inner join
          |  huarngpa_tmp_filtered_stock B
          |    on A.tweet_date = B.trading_day
          """.stripMargin
        );

        df.registerTempTable("huarngpa_tmp_join_twitter_stocks");
        df.write
          .mode(SaveMode.Overwrite)
          .format("hive")
          .saveAsTable("huarngpa_join_twitter_stocks");
        
      })
    })
    
    print("Completed.\n");
  }
  
  /*
   * Run the linear regression analysis
   */
  def batchViewsSentimentStockLinReg(): Unit = {


  }
  
  def main(args: Array[String]) = {
    
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
      //batchJoinTwitterAndStock()  // possibly broken
      //batchViewsSentimentStockLinReg()
      Thread.sleep(eightHours)
    }

  }
}
