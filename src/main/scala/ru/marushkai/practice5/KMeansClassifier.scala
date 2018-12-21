package ru.marushkai.practice5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KMeansClassifier {

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

    val inputDir = "./src/resources/tweets/"
    val outputDir = "./src/resources/tweetsModel/"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-tweets-train-kmeans")
      .master("local[*]")
      .getOrCreate()

    val jsonFiles = "[0-9]* ms/part-00000"

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(inputDir + jsonFiles)

    //Show the first 100 rows
    tweetsDF.show(100)

    //Extract the text
    val text = tweetsDF.select("text").rdd.map(r => r(0).toString)

    //Get the features vector
    val features = text.map(s => featurize(s))

    val numClusters = 10
    val numIterations = 40

    // Train KMenas model and save it to file
    val model: KMeansModel = KMeans.train(features, numClusters, numIterations)
    model.save(sparkSession.sparkContext, outputDir)
  }
}
