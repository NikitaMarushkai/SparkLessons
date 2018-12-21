package ru.marushkai.practice5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TweetsProcessor {

  //Featurize Function
  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val modelDir = "./src/resources/tweetsModel/"

    val conf = new SparkConf()
    conf.setAppName("language-classifier")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    CredentialsUtil.initializeTwitterCredentials()

    println("Creating twitter stream...")

    val realTimeTweets = TwitterUtils.createStream(ssc, None)

    val tweetsTexts = realTimeTweets.map(_.getText)

    println("Initialise KMeans model...")
    val model = KMeansModel.load(sc, modelDir)

    val langNumber = 1

    val filtered = tweetsTexts.filter(t => model.predict(featurize(t)) == langNumber)

    filtered.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
