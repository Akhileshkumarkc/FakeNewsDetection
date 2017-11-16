  import org.apache.spark.{SparkConf,SparkContext}
  import org.apache.spark.streaming.dstream.DStream
  import org.apache.spark.streaming.{Seconds,StreamingContext}
  import org.apache.spark.Logging
  import org.apache.spark.streaming.twitter.TwitterUtils
  

  object TwitterStream {
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
      //Split the stream and hastag extraction
      //val hashTags = stream.flatMap(status => status.getText.split(" "))//.filter(_.startsWith("#")))

      val topCounts60 = stream.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                       .map{case (topic, count) => (count, topic)}
                       .transform(_.sortByKey(false))
  
//      val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//                       .map{case (topic, count) => (count, topic)}
//                       .transform(_.sortByKey(false))
  
  
      // Print popular hashtags
      topCounts60.foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      })
      
//      topCounts10.foreachRDD(rdd => {
//        val topList = rdd.take(10)
//        println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
//        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//      })
  
      ssc.start()
      ssc.awaitTermination()
    }
  }



