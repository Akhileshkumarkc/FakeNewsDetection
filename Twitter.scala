package Fakenews

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
object Twitter {
  def main(args: Array[String]): Unit = {
    //setting up the configuration
    val conf = new SparkConf().setAppName("Twitterstream").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val filters = Seq("Trump")
    //sparkstreaming context
    val ssc = new StreamingContext(sc, Seconds(5))
    System.setProperty("twitter4j.oauth.consumerKey", "jrPlepQGYmtZkO4locnUwawHe")
    System.setProperty("twitter4j.oauth.consumerSecret", "JWREIWrENWcGt37FEyTPhfE34j4O1w6kkF02wCUhLB28blZ0nq")
    System.setProperty("twitter4j.oauth.accessToken", "899279922639675392-qWkTEtTiWJ6dYPrefliL21s2FkqWY6I")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "K6rXIUlCMl7HUFYPIXvbgC14DY4LrLtgxjIbGh5aavZWN")
    val stream = TwitterUtils.createStream(ssc, None, filters)
    //extracting URL which is contained in tweets
    val urlTweets=stream.map(_.getURLEntities()).flatMap(arrayURL=>arrayURL.filter(_.getExpandedURL().length>0)).map(value=>value.getExpandedURL).filter(!_.contains("twitter")).filter(!_.contains("youtu.be"))
    urlTweets.foreachRDD{value=> val spark=SparkSession.builder().config(value.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val tweetdf=value.toDF("tweeturl")
      tweetdf.createOrReplaceTempView("tweets")
      val tweets=spark.sql("select tweeturl from tweets")
      if(!tweets.rdd.isEmpty()){
      tweets.coalesce(1).write.mode(SaveMode.Append).text(args(0))}
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
