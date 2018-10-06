package ru.marushkai.practice2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSessionExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val jsonFile = "./src/resources/persons.json"

    val sparkSession = SparkSession
      .builder()
      .appName("Test spark SQL")
      .master("local[2]")
      .getOrCreate()

    val persons = sparkSession.read.json(jsonFile)

    persons.show()

    persons.printSchema()
  }
}
