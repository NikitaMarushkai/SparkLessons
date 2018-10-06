package ru.marushkai.practice2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkJsonDataframe {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val jsonFile = "./src/resources/sampletweets.json"

    val sparkSession = SparkSession
      .builder()
      .appName("Test spark SQL")
      .master("local[2]")
      .getOrCreate()

    import sparkSession.implicits._

    val persons = sparkSession.read.json(jsonFile)

    persons.show(5)

    persons.printSchema()

    println("User tweets")
    persons.select("body").show(10)

    println("10 Most active languages")
    persons.groupBy("actor.languages").count().orderBy($"count".desc).limit(10).show()

    //TODO: earliest and latest tweets

    println("10 top devices among tweeter users")
    persons.groupBy("generator.displayName").count().orderBy($"count".desc).limit(10).show()

    println("Tweets by user")
    persons.select("body", "actor.displayName").limit(10).show(10)

    println("How many tweets each user has")
    persons.select("actor.id", "body").groupBy("id").count().where($"count".gt(10))
      .orderBy($"count".desc).show(10)

//    persons.createOrReplaceTempView("persons")
//    sparkSession.sql("SELECT actor.displayName, count(body) GROUP BY displayName").show(100)
  }
}
