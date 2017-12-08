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
import org.jsoup.Jsoup
import org.jsoup.nodes._
import java.util.Iterator
import scala.collection.JavaConverters._
import java.net.{ URL, MalformedURLException }
import scala.util.control.Exception._
  

  object TwitterStream {
  
    def getText(url: String): String = {
        val html = scala.io.Source.fromURL(url).mkString
      
        var result = ""
        def processNode(node: Node){       
          if(node.isInstanceOf[TextNode] && !node.toString().equals(" "))
            result += node.toString()
          if(node.childNodeSize()>0)
            node.childNodes().asScala.foreach(x => processNode(x))
        }
      
        val doc = Jsoup.parse(html)
        processNode(doc)
        print(result)
        return result
        
    }
    def main(args: Array[String]): Unit = {
      //setting up the configuration
      val conf = new SparkConf().setAppName("Twitterstream").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val filters = Seq("Trump")
       
      sc.setLogLevel("OFF")
      //sparkstreaming context
      val ssc = new StreamingContext(sc, Seconds(2))
      System.setProperty("twitter4j.oauth.consumerKey", "jrPlepQGYmtZkO4locnUwawHe")
      System.setProperty("twitter4j.oauth.consumerSecret","JWREIWrENWcGt37FEyTPhfE34j4O1w6kkF02wCUhLB28blZ0nq")
      System.setProperty("twitter4j.oauth.accessToken", "899279922639675392-qWkTEtTiWJ6dYPrefliL21s2FkqWY6I")
      System.setProperty("twitter4j.oauth.accessTokenSecret", "K6rXIUlCMl7HUFYPIXvbgC14DY4LrLtgxjIbGh5aavZWN")
      val stream = TwitterUtils.createStream(ssc, None,filters)
      
      //extracting URL which is contained in tweets
      val urlTweets=stream.map(_.getURLEntities()).flatMap(arrayURL=>arrayURL.filter(_.getExpandedURL().length>0))
      .map(value=>value.getExpandedURL).filter(!_.contains("twitter")).filter(!_.contains("youtu.be"))
      
      urlTweets.foreachRDD{value => val spark=SparkSession.builder().config(value.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val tweetdf=value.toDF("tweeturl")
        tweetdf.createOrReplaceTempView("tweets")
        val tweets=spark.sql("select tweeturl from tweets").foreach(row => println(getText(row.getString(0))))
        
      }
  
      ssc.start()
      ssc.awaitTermination()
    }
  }



