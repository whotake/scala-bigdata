package Task5

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // Configure your Twitter credentials
    val apiKey = "PQR3Czqw0NMA2OexZcjqWcgGo"
    val apiSecret = "fffFOyP4fMxsZojmwnEjWAuhnxl6m20WuSIWKTx8r6Wv15SBHV"
    val accessToken = "924175083584851968-xxNTobKe92g9Rv8lq70Pr6RjFGZLvnv"
    val accessTokenSecret = "km0BMDFwL9EHlQ7wvv3pXd96OU4N0HN5Igzm3KbscdQhD"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)

    tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
