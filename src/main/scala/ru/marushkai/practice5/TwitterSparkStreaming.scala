package ru.marushkai.practice5

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterSparkStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf()
      .setAppName("spark-streaming")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(1))

    CredentialsUtil.initializeTwitterCredentials()

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(new Gson().toJson(_))

    val outputDir = "./src/resources/tweets/"

    DataDownloader.saveTweets(outputDir, tweets, true)


    ssc.start()
    ssc.awaitTermination()
  }
}
