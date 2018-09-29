import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TestPractise {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setAppName("Spark hello world")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val file = sc.textFile("./src/resources/super.csv")

    val splitted = file.map(_.split(";"))

    val heroCount = splitted.map(line => (line.lift(1), 1)).reduceByKey(_+_)

    val killedCount = splitted.map(line => (line.lift(1), line.lift(2).get.toInt)).reduceByKey(_+_)

    println("Hero count")
    heroCount.foreach(println)
    println("Killed count")
    killedCount.foreach(println)
  }
}
