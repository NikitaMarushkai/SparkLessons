package ru.marushkai.practice1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("Spark hello world")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val file = sc.textFile("./src/resources/products.csv")
    var words = file.flatMap(_.split(" ")).map((_, 1))
    words = words.reduceByKey(_ + _)
    words.foreach(println)
  }
}
