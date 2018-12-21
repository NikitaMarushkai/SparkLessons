package ru.marushkai.practice5

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import twitter4j.Status

object DataDownloader {
  def saveTweets(outputDir: String, tweets: DStream[String], isSave: Boolean) {
    val numTweetsCollect = 10000L
    var numTweetsCollected = 0L


    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.coalesce(1)
        outputRDD.saveAsTextFile(outputDir + time)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollect) {
          System.exit(0)
        }
      }
    })
  }
}
